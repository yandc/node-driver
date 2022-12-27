// Package detector to detect which node is the fastest.
package detector

import (
	"errors"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.bixin.com/mili/node-driver/common"
)

var (
	// ErrRingEnd all nodes have been tried.
	ErrRingEnd = errors.New("all nodes have been tried")

	// ErrRingOverflow no available nodes.
	ErrRingOverflow = errors.New("no avaiable nodes")

	// ErrRingEmpty no nodes.
	ErrRingEmpty = errors.New("no nodes")
)

// Node to detect.
type Node interface {
	// Detect run.
	Detect() error

	// URL returns url of node.
	URL() string

	// Recover handle panic.
	Recover(r interface{}) error

	// RetryAfter returns a timestamp indicates when this node is available to use.
	// Returns 0 will be always available.
	RetryAfter() time.Time
}

// Detector detector to detect nodes.
type Detector interface {
	// PickNth the nth of the fastest nodes. 0 is the 1st.
	// NOTE: This will not take effect of Failover.
	PickNth(nth int) (Node, error)

	// PickFastest Pick the fastest node.
	// You can mark the fastest node unavailable by invoking Failover.
	// Then the second fast node shall be returned.
	PickFastest() (Node, error)

	// Failover the fastest node.
	Failover() (newNth int)

	// WithRetry automaticlly retry with the detected priority.
	WithRetry(maxRetry int, fn func(node Node) error) error

	// EnableRoundRobin use all available nodes to invoke fn sequentially with retry.
	EnableRoundRobin()

	// Each to iterate over nodes, returns true in the callback to terminate.
	Each(func(i int, node Node) (terminate bool)) error

	// Len returns how many nodes.
	Len() int

	// Add add a node.
	Add(...Node)

	// Watch register a callback to watch nodes changed.
	Watch(...func([]Node))

	// WatchFailover register a callback to watch node had been failover.
	WatchFailover(...func(current Node, next Node))

	// WatchSuccess register a callback to watch node had been used successfully.
	WatchSuccess(...func(node Node))

	// StartDetectPlan a goroutine to detect which one is the fastest.
	StartDetectPlan(interval time.Duration, lastActiveBegin, lastActiveEnd time.Duration)

	// DetectAll detect all nodes.
	DetectAll()

	// DetectLastActiveBetween detect nodes that actived between a period.
	DetectLastActiveBetween(begin, end time.Duration)

	// SetPreferedNode set nodes that we are prefered to use.
	// NOTE: nodes will be added if they didn't add before.
	SetPreferedNode(node Node) bool

	// GetDetectingElapsed get how much time cost to do detecting of node.
	// NOTE: node have been added before.
	GetDetectingElapsed(node Node, refresh ...bool) (elapsed time.Duration, err error)
}

// WatchIn is a embeded struct to watch nodes changed by detector.
type WatchIn struct {
	lock       sync.RWMutex
	allRpcURLS []string
}

// OnNodeChanged should be registered to Detector by calling `Detector.Watch`.
func (in *WatchIn) OnNodeChanged(nodes []Node) {
	urls := make([]string, 0, len(nodes))
	for _, n := range nodes {
		urls = append(urls, n.URL())
	}
	in.lock.Lock()
	defer in.lock.Unlock()
	in.allRpcURLS = urls
}

// GetRPCURL returns the RPC url of node by given index.
func (in *WatchIn) GetRPCURL(i int) string {
	in.lock.RLock()
	defer in.lock.RUnlock()
	return in.allRpcURLS[i]
}

// URLNum returns how many urls.
func (in *WatchIn) URLNum() int {
	in.lock.RLock()
	defer in.lock.RUnlock()
	return len(in.allRpcURLS)
}

type simple struct {
	lock       *sync.RWMutex
	nodes      []*nodeWrapper
	watchers   []func([]Node)
	onFailover []func(Node, Node)
	onSuccess  []func(Node)
	startIdx   int32
	roundRobin int32
}

type nodeWrapper struct {
	node            Node
	detectElapsedMS int64
	workedAt        int64

	// set this when node failed but its RetryAfter hasn't changed accorrdingly.
	defaultRetryAfter    *time.Time
	numOfContinualFailed uint32
}

func (w *nodeWrapper) doDetect() error {
	start := time.Now()
	if err := w.node.Detect(); err != nil {
		w.SetDetectElapsed(time.Hour) // max
		return err
	}
	w.SetDetectElapsed(time.Now().Sub(start))
	return nil
}

func (w *nodeWrapper) WorkedAt() time.Time {
	return time.Unix(atomic.LoadInt64(&w.workedAt), 0)
}

func (w *nodeWrapper) UpdateWorkedAt() {
	atomic.StoreInt64(&w.workedAt, time.Now().Unix())
	atomic.StoreUint32(&w.numOfContinualFailed, 0)
}

func (w *nodeWrapper) DetectElapsed() time.Duration {
	return time.Duration(time.Millisecond * time.Duration(atomic.LoadInt64(&w.detectElapsedMS)))
}

func (w *nodeWrapper) SetDetectElapsed(elapsed time.Duration) {
	atomic.StoreInt64(&w.detectElapsedMS, int64(elapsed/time.Millisecond))
}

func (w *nodeWrapper) retryAfter() time.Time {
	if w.defaultRetryAfter != nil {
		return *w.defaultRetryAfter
	}
	return w.node.RetryAfter()
}

func (w *nodeWrapper) failed() {
	num := atomic.AddUint32(&w.numOfContinualFailed, 1)
	if w.node.RetryAfter().UnixNano() < time.Now().UnixNano() {
		// retry after not update accordingly, set defaultRetryAfter.
		retryAfter := time.Now().Add(time.Second * time.Duration(1<<num))
		w.defaultRetryAfter = &retryAfter
	} else {
		w.defaultRetryAfter = nil
	}
	if num >= 5 {
		atomic.StoreUint32(&w.numOfContinualFailed, 0)
	}
}

// NewSimpleDetector create a simple contest.
func NewSimpleDetector() Detector {
	return &simple{
		lock:       &sync.RWMutex{},
		nodes:      make([]*nodeWrapper, 0, 4),
		watchers:   make([]func([]Node), 0, 4),
		onFailover: make([]func(Node, Node), 0, 4),
		onSuccess:  make([]func(Node), 0, 4),
	}
}

func (h *simple) PickFastest() (Node, error) {
	if len(h.nodes) == 0 {
		return nil, ErrRingEmpty
	}
	startIdx := atomic.LoadInt32(&h.startIdx)

	return h.nthNode(int(startIdx)).node, nil
}

func (h *simple) PickNth(nth int) (Node, error) {
	if nth >= len(h.nodes) {
		return nil, ErrRingOverflow
	}
	return h.nthNode(nth).node, nil
}

func (h *simple) Failover() int {
	idx := atomic.LoadInt32(&h.startIdx)
	newIdx := idx + 1

	// The ring is end and reset it to zero.
	if int(newIdx) >= len(h.nodes) {
		newIdx = 0
	}
	if atomic.CompareAndSwapInt32(&h.startIdx, idx, newIdx) {
		return int(newIdx)
	} else {
		return int(atomic.LoadInt32(&h.startIdx))
	}
}

func (h *simple) Add(nodes ...Node) {
	h.lock.Lock()
	defer h.lock.Unlock()
	for _, node := range nodes {
		w := &nodeWrapper{
			node: node,
		}
		h.nodes = append(h.nodes, w)
	}
}

func (h *simple) Each(each func(i int, n Node) bool) error {
	return h.each(func(i int, idx int, n *nodeWrapper) bool {
		return each(i, n.node)
	})
}

func (h *simple) each(each func(i int, idx int, n *nodeWrapper) bool) error {
	nodes := h.copyNodeWrappers()
	length := len(nodes)
	if length == 0 {
		return ErrRingEmpty
	}

	startIdx := int(atomic.LoadInt32(&h.startIdx))
	availableNodes := make([]*nodeWrapper, length)
	var nextAvailNode *nodeWrapper
	var anyNodeAvailable bool
	now := time.Now()

	for i := 0; i < length; i++ {
		idx := startIdx + i
		if idx >= length {
			idx = idx - length
		}
		node := nodes[idx]
		retryAfter := node.retryAfter()
		if retryAfter.Sub(now) > 0 {
			availableNodes[i] = nil
		} else {
			availableNodes[i] = node
			anyNodeAvailable = true
		}
		if nextAvailNode == nil || nextAvailNode.retryAfter().UnixNano() > retryAfter.UnixNano() {
			nextAvailNode = node
		}
	}

	// no node is available.
	if !anyNodeAvailable {
		// sleep to wait node available.
		time.Sleep(nextAvailNode.retryAfter().Sub(now))

		// some nodes should be available after sleep, now add them as available nodes.
		for i := 0; i < length; i++ {
			idx := startIdx + i
			if idx >= length {
				idx = idx - length
			}
			node := nodes[idx]
			// we only compare on seconds to make as many nodes available as possible.
			if node.retryAfter().Unix() == nextAvailNode.retryAfter().Unix() {
				availableNodes[i] = node
			}
		}
	}

	for i, node := range availableNodes {
		if node == nil { // node unavailable will set be nil
			continue
		}

		idx := startIdx + i
		if terminate := each(i, idx, node); terminate {
			return nil
		}
	}
	return nil
}

func (h *simple) nthNode(idx int) *nodeWrapper {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return h.nodes[idx]
}

func (h *simple) Len() int {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return len(h.nodes)
}

func (h *simple) EnableRoundRobin() {
	atomic.StoreInt32(&h.roundRobin, 1)
}

func (h *simple) WithRetry(maxRetry int, fn func(Node) error) (ret error) {
	if atomic.LoadInt32(&h.roundRobin) == 1 {
		h.Failover() // failover to use a new node for round robin purpose.
	}

	err := h.each(func(i int, idx int, node *nodeWrapper) (terminate bool) {
		if i >= maxRetry {
			return true // terminate
		}

		// Invoke without error and we shall done the progress.
		if ret = h.do(node.node, fn); ret == nil {
			// set detected to avoid detect in plan.
			node.UpdateWorkedAt()

			// Node have been used successfully.
			for _, f := range h.onSuccess {
				f(node.node)
			}
			return true // terminate Each

		}
		node.failed()

		if !common.NeedRetry(ret) {
			return true // terminate Each
		}

		// Mark current node unavailable.
		// Skip if current node is already failovered: fit in invoking concurrently.
		if idx == int(atomic.LoadInt32(&h.startIdx)) {
			newIdx := h.Failover()
			for _, f := range h.onFailover {
				f(node.node, h.nthNode(newIdx).node)
			}
		}
		return false
	})
	if ret == nil {
		ret = err
	}
	return ret
}

func (h *simple) do(node Node, fn func(Node) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = node.Recover(r)
		}
	}()

	return fn(node)
}

func (h *simple) Watch(watchers ...func([]Node)) {
	h.watchers = append(h.watchers, watchers...)
}

func (h *simple) WatchFailover(watchers ...func(Node, Node)) {
	h.onFailover = append(h.onFailover, watchers...)
}

func (h *simple) WatchSuccess(watchers ...func(Node)) {
	h.onSuccess = append(h.onSuccess, watchers...)
}

func (h *simple) StartDetectPlan(interval time.Duration, lastActiveBegin, lastActiveEnd time.Duration) {
	tick := time.NewTicker(interval)
	for {
		<-tick.C
		h.DetectLastActiveBetween(lastActiveBegin, lastActiveEnd)
	}
}

type toSort struct {
	node Node
	dur  time.Duration
}

func (h *simple) DetectAll() {
	h.DetectLastActiveBetween(0, math.MaxInt64)
}

func (h *simple) DetectLastActiveBetween(begin, end time.Duration) {
	nodes := h.copyNodeWrappers()
	nodes = h.doDetect(nodes, begin, end)

	h.lock.Lock()
	// No new node has been added, it's safe to override,
	// as we're not support remove or replace node yet.
	if len(h.nodes) != len(nodes) {
		h.nodes = nodes
	}
	h.lock.Unlock()

	// Reset the ring starts over.
	atomic.StoreInt32(&h.startIdx, 0)

	h.notifyNodeChanged()
}

func (h *simple) doDetect(nodes []*nodeWrapper, begin, end time.Duration) []*nodeWrapper {
	wg := &sync.WaitGroup{}

	for idx, n := range nodes {
		workedAt := n.WorkedAt()
		if !workedAt.IsZero() {
			sinceWorkedAt := time.Now().Sub(workedAt)
			if !(sinceWorkedAt >= begin && sinceWorkedAt <= end) {
				continue
			}
		}

		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			nw := h.nodes[idx]
			_ = nw.doDetect()
		}(idx)
	}
	wg.Wait()

	// Sort by elapsed time in incr order.
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].DetectElapsed() < nodes[j].DetectElapsed() // Incr
	})
	return nodes
}

func (h *simple) notifyNodeChanged() {
	nodes := h.copyNodes()

	for _, w := range h.watchers {
		w(nodes)
	}
}

func (h *simple) copyNodes() []Node {
	wrappers := h.copyNodeWrappers()
	nodes := make([]Node, 0, len(wrappers))
	for _, w := range wrappers {
		nodes = append(nodes, w.node)
	}
	return nodes
}

func (h *simple) copyNodeWrappers() []*nodeWrapper {
	h.lock.RLock()
	defer h.lock.RUnlock()
	nodes := make([]*nodeWrapper, 0, len(h.nodes))
	for _, n := range h.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

// SetPreferedNode set nodes that we are prefered to use.
// NOTE: nodes will be added if they didn't add before.
func (h *simple) SetPreferedNode(node Node) bool {
	nodes := h.copyNodeWrappers()
	for idx, n := range nodes {
		if n.node.URL() == node.URL() {
			startIdx := atomic.LoadInt32(&h.startIdx)
			if startIdx == int32(idx) {
				return true
			}
			n.UpdateWorkedAt()
			return atomic.CompareAndSwapInt32(&h.startIdx, startIdx, int32(idx))
		}
	}
	h.Add(node)
	startIdx := atomic.LoadInt32(&h.startIdx)
	return atomic.CompareAndSwapInt32(&h.startIdx, startIdx, int32(h.Len()-1))
}

// GetDetectingElapsed get how much time cost to do detecting of node.
// NOTE: node have been added before.
func (h *simple) GetDetectingElapsed(node Node, refresh ...bool) (elapsed time.Duration, err error) {
	nodes := h.copyNodeWrappers()
	for _, n := range nodes {
		if n.node.URL() == node.URL() {
			if len(refresh) > 0 && refresh[0] {
				err = n.doDetect()
			}

			elapsed := n.DetectElapsed()
			if elapsed == time.Hour {
				return 0, errors.New("detect failed")
			}
			return n.DetectElapsed(), err
		}
	}
	return 0, errors.New("not found")
}
