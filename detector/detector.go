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

	// StartDetectPlan a goroutine to detect which one is the fastest.
	StartDetectPlan(interval time.Duration, lastActiveBegin, lastActiveEnd time.Duration)

	// DetectAll detect all nodes.
	DetectAll()

	// DetectLastActiveBetween detect nodes that actived between a period.
	DetectLastActiveBetween(begin, end time.Duration)
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
	startIdx   int32
}

type nodeWrapper struct {
	node          Node
	detectElapsed time.Duration
	detectedAt    time.Time
}

func (w *nodeWrapper) DetectedAt() time.Time {
	return w.detectedAt
}

func (w *nodeWrapper) UpdateDetectedAt() {
	w.detectedAt = time.Now()
}

func (w *nodeWrapper) DetectElapsed() time.Duration {
	return w.detectElapsed
}

func (w *nodeWrapper) SetDetectElapsed(elapsed time.Duration) {
	w.detectElapsed = elapsed
	w.detectedAt = time.Now()
}

// NewSimpleDetector create a simple contest.
func NewSimpleDetector() Detector {
	return &simple{
		lock:       &sync.RWMutex{},
		nodes:      make([]*nodeWrapper, 0, 4),
		watchers:   make([]func([]Node), 0, 4),
		onFailover: make([]func(Node, Node), 0, 4),
	}
}

func (h *simple) PickFastest() (Node, error) {
	if len(h.nodes) == 0 {
		return nil, ErrRingEmpty
	}
	startIdx := atomic.LoadInt32(&h.startIdx)

	return h.nodes[startIdx].node, nil
}

func (h *simple) PickNth(nth int) (Node, error) {
	if nth >= len(h.nodes) {
		return nil, ErrRingOverflow
	}
	return h.nodes[nth].node, nil
}

func (h *simple) Failover() int {
	newIdx := atomic.AddInt32(&h.startIdx, 1)

	// The ring is end and reset it to zero.
	if int(newIdx) >= len(h.nodes) {
		atomic.StoreInt32(&h.startIdx, 0)
		return 0
	}
	return int(newIdx)
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
	return h.each(func(i int, n *nodeWrapper) bool {
		return each(i, n.node)
	})
}

func (h *simple) each(each func(i int, n *nodeWrapper) bool) error {
	h.lock.RLock()
	defer h.lock.RUnlock()

	length := h.Len()
	if length == 0 {
		return ErrRingEmpty
	}

	startIdx := int(atomic.LoadInt32(&h.startIdx))
	for i := 0; i < length; i++ {
		idx := startIdx + i
		if idx >= length {
			idx = idx - length
		}
		n := h.nodes[idx]
		if terminate := each(i, n); terminate {
			return nil
		}
	}
	return nil
}

func (h *simple) Len() int {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return len(h.nodes)
}

func (h *simple) WithRetry(maxRetry int, fn func(Node) error) (ret error) {
	err := h.each(func(i int, node *nodeWrapper) (terminate bool) {
		if i >= maxRetry {
			return true // terminate
		}
		// Invoke without error and we shall done the progress.
		if ret = h.do(node.node, fn); ret == nil {
			// set detected to avoid detect in plan.
			node.UpdateDetectedAt()
			return true // terminate Each

		}

		if !common.NeedRetry(ret) {
			return true // terminate Each
		}

		// mark current node unavailable.
		newIdx := h.Failover()
		for _, f := range h.onFailover {
			f(h.nodes[i].node, h.nodes[newIdx].node)
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
			if strValue, ok := r.(string); ok {
				err = errors.New(strValue)
			} else if errValue, ok := r.(error); ok {
				err = errValue
			}
			return
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
	h.lock.Lock()
	defer h.lock.Unlock()

	wg := &sync.WaitGroup{}

	for idx, n := range h.nodes {
		detectedAt := n.DetectedAt()
		if !detectedAt.IsZero() {
			sinceLastActive := time.Now().Sub(detectedAt)
			if !(sinceLastActive >= begin && sinceLastActive <= end) {
				continue
			}
		}

		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			nw := h.nodes[idx]
			start := time.Now()
			if err := nw.node.Detect(); err != nil {
				nw.SetDetectElapsed(time.Hour) // max
				return
			}
			nw.SetDetectElapsed(time.Now().Sub(start))
		}(idx)
	}
	wg.Wait()

	// Sort by elapsed time in incr order.
	sort.Slice(h.nodes, func(i, j int) bool {
		return h.nodes[i].DetectElapsed() < h.nodes[j].DetectElapsed() // Incr
	})

	nodes := make([]Node, 0, len(h.nodes))

	for _, n := range h.nodes {
		nodes = append(nodes, n.node)
	}

	for _, w := range h.watchers {
		w(nodes)
	}

	// Reset the ring starts over.
	atomic.StoreInt32(&h.startIdx, 0)
}
