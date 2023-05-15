package chain

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.bixin.com/mili/node-driver/common"
	"gitlab.bixin.com/mili/node-driver/detector"
)

var (
	// ErrNoCurrentHeight can't load height from cache or DB.
	ErrNoCurrentHeight = errors.New("we don't start yet, go from the newest height")

	// ErrSlowBlockHandling slow block handling
	ErrSlowBlockHandling = errors.New("slow block handling")

	// ErrForkedZeroBlockNumber block forked but block number is 0.
	ErrForkedZeroBlockNumber = errors.New("block forked but number is 0")

	// ErrUnexpectedBlockNumber got unexpected block number.
	ErrUnexpectedBlockNumber = errors.New("got unexpected block number")
)

// TxType transaction type.
type TxType string

const (
	TxTypeTransfer     TxType = "transfer"
	TxTypeApprove             = "approve"
	TxTypeTransferFrom        = "transferfrom"
	TxTypeContract            = "contract"
	TxTypeNative              = "native"
)

// Clienter node client interface.
type Clienter interface {
	detector.Node

	// GetBlock fetch block data of the given height.
	GetBlock(height uint64) (*Block, error)

	// GetTxByHash get transaction by given tx hash.
	GetTxByHash(txHash string) (tx *Transaction, err error)
}

// StateStore to store block height.
type StateStore interface {
	// StoreHeight store height to crawl.
	StoreHeight(height uint64) error

	// StoreNodeHeight store ndoe height for monitor purpose.
	StoreNodeHeight(height uint64) error

	// LoadHeight load height to crawl.
	LoadHeight() (height uint64, err error)

	// StoreBlockHash store block hash by height.
	StoreBlockHash(height uint64, blockHash string) error

	// LoadBlockHash load block hash by height.
	LoadBlockHash(height uint64) (string, error)

	// LoadPendingTxs load blocks that are still pending.
	LoadPendingTxs() (txs []*Transaction, err error)
}

// Block block.
type Block struct {
	Hash       string
	ParentHash string
	Number     uint64
	Nonce      uint64
	BaseFee    string
	Time       int64

	Raw interface{}

	Transactions []*Transaction
	Metrics      []*Metric

	// ExtraTxs may come from contract logs that will be added in OnNewTx invocation..
	ExtraTxs []*Transaction
}

// Transaction transaction.
type Transaction struct {
	Hash        string
	Nonce       uint64
	BlockNumber uint64
	TxType      TxType
	FromAddress string
	ToAddress   string
	Value       string

	Result *TxResult

	Raw    interface{}
	Record interface{}
}

// TxResult the result of tx.
type TxResult struct {
	MatchedFrom   bool // Matched the from uid from our system.
	MatchedTo     bool // Matched the to uid from our system.
	FailedOnChain bool // Tx has been processed successfully by chain.
	FromUID       string
}

// SetResult
func (tx *Transaction) SetResult(matchedFrom, matchedTo, failedOnChain bool, fromUID string) {
	tx.Result = &TxResult{
		MatchedFrom:   matchedFrom,
		MatchedTo:     matchedTo,
		FailedOnChain: failedOnChain,
		FromUID:       fromUID,
	}
}

// Metric metric
type Metric struct {
	Stage    string
	NodeURLs []string
	Begin    time.Time
	End      time.Time
}

// Elapsed .
func (m *Metric) Elapsed() time.Duration {
	return m.End.Sub(m.Begin)
}

// BlockHandler to handle block.
type BlockHandler interface {
	// BlockInterval returns how long the system take to produce a block.
	BlockInterval() time.Duration

	// BlockMayFork returns true if the block may be forked.
	BlockMayFork() bool

	// OnNewBlock will be invoked when crawl new block.
	OnNewBlock(client Clienter, chainHeight uint64, block *Block) (TxHandler, error)

	// CreateTxHandler create TxHandler directly without a block.
	// Will be invoked when seal pending txs and tx is not on the chain.
	CreateTxHandler(client Clienter, tx *Transaction) (TxHandler, error)

	// OnForkedBlock will be invoked when determine block has been forked.
	OnForkedBlock(client Clienter, block *Block) error

	// OnError will be invoked when error occurred.
	// Returns true will ignore current block.
	OnError(err error, optHeight ...HeightInfo) (incrHeight bool)

	// WrapsError wraps error to control when to retry.
	WrapsError(client Clienter, err error) error

	// IsDroppedTx returns true if tx had been dropped on-chain.
	// `txByHash` and `err` are returned by `client.GetTxByHash`
	IsDroppedTx(txByHash *Transaction, err error) bool
}

// HeightInfo info of heights.
type HeightInfo struct {
	ChainHeight uint64
	CurHeight   uint64
}

// TxHandler to handle txs.
type TxHandler interface {
	// OnNewTx will be invoked when retrieved new tx from block.
	OnNewTx(client Clienter, block *Block, tx *Transaction) error

	// OnDroppedTx will be invoked when Clienter.GetTxByHash returns nil without error.
	OnDroppedTx(client Clienter, tx *Transaction) error

	// OnSealedTx will be invoked when Clienter.GetTxByHash returns non-nil without error.
	OnSealedTx(client Clienter, tx *Transaction) error

	// Save will be invoked after muliple invocations of above.
	// WILL NOT INVOKE CONCURRENTLY.
	Save(client Clienter) error
}

// DetectorWatcher to watch nodes changed.
type DetectorWatcher interface {
	OnNodesChange([]detector.Node)
	OnNodeFailover(current detector.Node, next detector.Node)
	OnNodeSuccess(node detector.Node)
}

type Watcher interface {
	DetectorWatcher

	// OnSentTxFailed will be invoked when txs have failed and sent by us.
	//
	// - totalInTenMins: number of total failed txs in 10 minutes.
	// - numOfContinuous: number of continuous failed txs.
	OnSentTxFailed(txType TxType, numOfContinuous int, txs []*Transaction)
}

// DefaultTxDroppedIn default implementation of `IsDroppedTx`
type DefaultTxDroppedIn struct {
}

func (td *DefaultTxDroppedIn) IsDroppedTx(txByHash *Transaction, err error) bool {
	return txByHash == nil && err == nil
}

// BlockSpider block spider to crawl blocks from chain.
type BlockSpider struct {
	detectors   [2]detector.Detector
	detectorIdx int32
	standby     detector.Detector
	store       StateStore
	watchers    []Watcher

	concurrencyTxs int

	accumulator sentTxFailedAccumulator

	prefetchedBlocks map[uint64]*Block
	prefetchedTxs    map[string]*Transaction
}

type blockTxHandler struct {
	block *Block
	inner TxHandler
}

type getBlocksOpt struct {
	localHeight        uint64
	chainHeight        uint64
	concurrentDeltaThr int
	maxConcurrency     int
}

func (opt getBlocksOpt) heightDelta() int {
	return int(opt.chainHeight - opt.localHeight)
}

// safelyConcurrent returns true if it's safe to crawl blocks concurrently(no forked blocks).
func (opt getBlocksOpt) safelyConcurrent() bool {
	// Won't crawl blocks concurrently if delta is 0.
	if opt.concurrentDeltaThr <= 0 {
		return false
	}

	return opt.heightDelta() > opt.concurrentDeltaThr
}

func (opt getBlocksOpt) concurrency() int {
	if opt.maxConcurrency <= 0 {
		return 1
	}

	concurrency := opt.heightDelta() - opt.concurrentDeltaThr
	if concurrency > opt.maxConcurrency {
		concurrency = opt.maxConcurrency
	}
	if concurrency < 1 {
		concurrency = 1
	}
	return concurrency
}

// NewBlockSpider create platform.
func NewBlockSpider(store StateStore, clients ...Clienter) *BlockSpider {
	d := detector.NewSimpleDetector()

	nodes := make([]detector.Node, 0, len(clients))
	for _, c := range clients {
		nodes = append(nodes, c)
	}
	d.Add(nodes...)

	return &BlockSpider{
		detectors: [2]detector.Detector{d, nil},
		store:     store,
		standby:   detector.NewSimpleDetector(),
	}
}

func (b *BlockSpider) detector() detector.Detector {
	return b.detectors[atomic.LoadInt32(&b.detectorIdx)]
}

func (b *BlockSpider) SetStateStore(store StateStore) {
	b.store = store
}

func (b *BlockSpider) SetPrefetched(blocks []*Block, txs []*Transaction) {
	b.prefetchedBlocks = make(map[uint64]*Block)
	b.prefetchedTxs = make(map[string]*Transaction)

	for _, blk := range blocks {
		b.prefetchedBlocks[blk.Number] = blk
	}
	for _, tx := range txs {
		b.prefetchedTxs[tx.Hash] = tx
	}
}

func (b *BlockSpider) EnableRoundRobin() {
	b.detector().EnableRoundRobin()
}

func (b *BlockSpider) SetHandlingTxsConcurrency(v int) {
	if v < 1 {
		v = 1
	}

	b.concurrencyTxs = v
}

func (b *BlockSpider) AddStandby(clients ...Clienter) {
	nodes := make([]detector.Node, 0, len(clients))
	for _, c := range clients {
		nodes = append(nodes, c)
	}
	b.standby.Add(nodes...)
}

func (b *BlockSpider) Start(handler BlockHandler, concurrentDeltaThr int, maxConcurrency int) {
	go b.StartIndexBlock(handler, concurrentDeltaThr, maxConcurrency)
}

func (b *BlockSpider) Watch(watchers ...Watcher) {
	b.watchers = watchers
	b.doWatch()
}

func (b *BlockSpider) doWatch() {
	dWatchers := make([]DetectorWatcher, 0, len(b.watchers))
	for _, w := range b.watchers {
		dWatchers = append(dWatchers, w)
	}
	b.WatchDetector(dWatchers...)
}

func (b *BlockSpider) startDetect() {
	b.detector().DetectAll()
	b.detector().StartDetectPlan(3*time.Minute, 0, 3*time.Minute)
	b.detector().StartDetectPlan(10*time.Minute, 3*time.Minute, 10*time.Minute)
	b.detector().StartDetectPlan(20*time.Minute, 10*time.Minute, math.MaxInt64)

}

// ReplaceClients assume this invocation will be used in a very low frequency
// without concurrency.
func (b *BlockSpider) ReplaceClients(clients ...Clienter) {
	// Stop the background goroutines that started by current.
	b.detector().StopDetectPlans()

	// Create a new detector.
	d := detector.NewSimpleDetector()

	if b.detector().IsRoundRobinEnabled() {
		d.EnableRoundRobin()
	}

	nodes := make([]detector.Node, 0, len(clients))
	for _, c := range clients {
		nodes = append(nodes, c)
	}
	d.Add(nodes...)

	// Store the new detector to the unused position of the array.
	idx := atomic.LoadInt32(&b.detectorIdx)
	unusedIdx := (idx + 1) % 2
	b.detectors[unusedIdx] = d

	// Change the index of current detector to use the new detector.
	atomic.StoreInt32(&b.detectorIdx, unusedIdx)

	// Receover watchers and detect plans.
	b.doWatch()
	b.startDetect()
}

func (b *BlockSpider) WatchDetector(watchers ...DetectorWatcher) {
	for _, w := range watchers {
		b.detector().Watch(w.OnNodesChange)
		b.detector().WatchFailover(w.OnNodeFailover)
		b.detector().WatchSuccess(w.OnNodeSuccess)
	}
}

// StartIndexBlock starts a loop to index blocks: extract transactions from block.
//
// - concurrentDeltaThr indicates how many blocks delta from chain is safe to get blocks concurrently.
// - maxConcurrency indicates the max goroutines to get blocks concurrently.
func (b *BlockSpider) StartIndexBlock(handler BlockHandler, concurrentDeltaThr int, maxConcurrency int) {
	b.StartIndexBlockWithContext(context.Background(), handler, concurrentDeltaThr, maxConcurrency)
}

// StartIndexBlock starts a loop to index blocks: extract transactions from block.
//
// - concurrentDeltaThr indicates how many blocks delta from chain is safe to get blocks concurrently.
// - maxConcurrency indicates the max goroutines to get blocks concurrently.
func (b *BlockSpider) StartIndexBlockWithContext(ctx context.Context, handler BlockHandler, concurrentDeltaThr int, maxConcurrency int) {
	b.startDetect()
	for {
		height, curHeight, err := b.getHeights(handler)
		if err != nil {
			handler.OnError(err)
			continue
		}

		b.store.StoreNodeHeight(height)

		if curHeight > height {
			time.Sleep(handler.BlockInterval())
			continue
		}

		// 1000 个块之后重新获取链上块高。
		nextHeight := height
		if nextHeight-curHeight > 1000 {
			nextHeight = curHeight + 1000
		}

		for curHeight <= nextHeight {
			opt := getBlocksOpt{
				localHeight:        curHeight,
				chainHeight:        height,
				concurrentDeltaThr: concurrentDeltaThr,
				maxConcurrency:     maxConcurrency,
			}

			indexedBlockNum, storeHeight := b.doIndexBlocks(handler, height, &opt)

			if !storeHeight {
				goto _END_INNER_LOOP
			}

			if err := b.store.StoreHeight(opt.localHeight + uint64(indexedBlockNum)); err != nil {
				handler.OnError(err, HeightInfo{
					ChainHeight: opt.chainHeight,
					CurHeight:   opt.localHeight,
				})
				goto _END_INNER_LOOP
			}
			curHeight += uint64(indexedBlockNum)
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	_END_INNER_LOOP:
		select {
		case <-ctx.Done():
			b.detector().StopDetectPlans()
			return
		default:
		}
	}
}

func (b *BlockSpider) handleErrs(handler BlockHandler, errs map[uint64]error, opt *getBlocksOpt) (storeHeight bool) {
	storeHeight = true
	for curHeight, err := range errs {
		storeHeight = storeHeight && handler.OnError(err, HeightInfo{
			ChainHeight: opt.chainHeight,
			CurHeight:   curHeight,
		})
	}
	return storeHeight
}

func (b *BlockSpider) doIndexBlocks(handler BlockHandler, chainHeight uint64, opt *getBlocksOpt) (indexedBlockNum int, storeHeight bool) {
	blocks, errs := b.getBlocks(opt, handler)

	if !b.handleErrs(handler, errs, opt) {
		return 0, false
	}
	indexedBlockNum = len(blocks) + len(errs)

	errs = make(map[uint64]error)

	txHandlers := make([]*blockTxHandler, 0, len(blocks))
	var (
		wg   sync.WaitGroup
		lock sync.Mutex
	)

	for _, blk := range blocks {
		wg.Add(1)
		go func(block *Block) {
			defer wg.Done()
			start := time.Now()
			txHandler, err := b.handleTx(block, chainHeight, handler, opt)
			block.Metrics = append(block.Metrics, &Metric{
				Stage: "handleTx",
				Begin: start,
				End:   time.Now(),
			})

			lock.Lock()
			defer lock.Unlock()

			if err != nil {
				errs[block.Number] = err
				return
			}

			txHandlers = append(txHandlers, &blockTxHandler{
				block: block,
				inner: txHandler,
			})
		}(blk)
	}

	wg.Wait()
	if !b.handleErrs(handler, errs, opt) {
		return 0, false
	}

	sort.Slice(txHandlers, func(i, j int) bool {
		return txHandlers[i].block.Number < txHandlers[j].block.Number
	})

	var err error
	// XXX Using this style of for-loop to break when err is not nil.
	for i := 0; i < len(txHandlers) && err == nil; i++ {
		txHandler := txHandlers[i]
		start := time.Now()

		err = b.WithRetry(func(client Clienter) error {
			return handler.WrapsError(client, txHandler.inner.Save(client))
		})
		txHandler.block.Metrics = append(txHandler.block.Metrics, &Metric{
			Stage: "save",
			Begin: start,
			End:   time.Now(),
		})
		if err != nil {
			errs[txHandler.block.Number] = err
		}
	}

	// 保存对应块高 hash
	for _, blk := range blocks {
		start := time.Now()
		if err := b.store.StoreBlockHash(blk.Number, blk.Hash); err != nil {
			errs[blk.Number] = err
		}
		blk.Metrics = append(blk.Metrics, &Metric{
			Stage: "storeHash",
			Begin: start,
			End:   time.Now(),
		})
		b.warnSlow(handler, blk)
	}
	if !b.handleErrs(handler, errs, opt) {
		return 0, false
	}

	for _, blk := range blocks {
		b.accumulator.accumulateBlock(blk)
	}

	b.monitorContinuousSentTxFailure()
	return indexedBlockNum, true
}

func (b *BlockSpider) getHeights(handler BlockHandler) (height, curHeight uint64, err error) {
	err = b.WithRetry(func(client Clienter) error {
		height, err = client.GetBlockHeight()
		return handler.WrapsError(client, err)
	})

	if err != nil {
		return
	}

	curHeight, err = b.store.LoadHeight()
	if err == ErrNoCurrentHeight {
		curHeight = height
		err = nil
	}
	return
}

func (b *BlockSpider) getBlocks(opt *getBlocksOpt, handler BlockHandler) ([]*Block, map[uint64]error) {
	if opt.safelyConcurrent() && opt.concurrency() > 1 {
		return b.concurrentGetBlocks(opt.localHeight, opt.chainHeight, handler, opt.concurrency())
	}
	errs := make(map[uint64]error)
	block, err := b.getBlock(opt.localHeight, handler)
	if err != nil {
		errs[opt.localHeight] = err
		return nil, errs
	}
	if handler.BlockMayFork() {
		var newHeight uint64
		block, newHeight, err = b.handleBlockMayFork(opt, handler, block)
		if err != nil {
			errs[opt.localHeight] = err
			return nil, errs
		}
		// 从新的块高开始重新爬取getBlocks
		opt.localHeight = newHeight
	}
	blocks := make([]*Block, 0, 1)
	blocks = append(blocks, block)
	return blocks, nil
}

// 1. Start multiple goroutines to get blocks;
// 2. Sort the blocks in incr order;
// 3. Store hashes of the blocks to the store;
func (b *BlockSpider) concurrentGetBlocks(curHeight, height uint64, handler BlockHandler, concurrency int) ([]*Block, map[uint64]error) {
	var wg sync.WaitGroup
	var lock sync.Mutex
	blocks := make([]*Block, 0, concurrency)
	errs := make(map[uint64]error)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(height uint64) {
			defer wg.Done()
			block, err := b.getBlock(height, handler)

			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				errs[height] = err
				return
			}
			blocks = append(blocks, block)
		}(curHeight + uint64(i))
	}
	wg.Wait()

	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Number < blocks[j].Number
	})

	if handler.BlockMayFork() {
		//XXX:  With concurrentDeltaThr there should no fork blocks.
	}

	return blocks, errs
}

func (b *BlockSpider) getBlock(height uint64, handler BlockHandler) (block *Block, err error) {
	start := time.Now()
	nodeURLs := make([]string, 0, b.detector().Len())
	err = b.WithRetry(func(client Clienter) error {
		nodeURLs = append(nodeURLs, client.URL())
		if blk, ok := b.prefetchedBlocks[height]; ok {
			block = blk
			return nil
		}
		block, err = client.GetBlock(height)
		return handler.WrapsError(client, err)
	})
	if block != nil {
		if block.Number != height {
			return nil, ErrUnexpectedBlockNumber
		}
		block.Metrics = append(block.Metrics, &Metric{
			Stage:    "retrieveBlock",
			NodeURLs: nodeURLs,
			Begin:    start,
			End:      time.Now(),
		})
	}
	return
}

func (b *BlockSpider) handleBlockMayFork(opt *getBlocksOpt, handler BlockHandler, inBlock *Block) (block *Block, curHeight uint64, err error) {
	block = inBlock
	curHeight = opt.localHeight

	err = b.WithRetry(func(client Clienter) error {
		for b.isForkedBlock(curHeight, block) {
			curHeight = curHeight - 1
			if block.Number <= 0 {
				return handler.WrapsError(client, ErrForkedZeroBlockNumber)
			}
			if opt.chainHeight-block.Number > uint64(opt.concurrentDeltaThr) {
				// 这里只调用 WrapsError 触发报警，不阻塞流程。
				handler.WrapsError(client, &ForkDeltaOverflow{
					ChainHeight: opt.chainHeight,
					BlockNumber: block.Number,
					SafelyDelta: uint64(opt.concurrentDeltaThr),
				})
			} else {
				// 安全块高内调用 OnForkedBlock，否则只回退块高不进行 OnForkedBlock 调用。
				if err := handler.OnForkedBlock(client, block); err != nil {
					return handler.WrapsError(client, err)
				}
			}
			// TODO: Store height.
			if blk, err := client.GetBlock(curHeight); err != nil {
				return handler.WrapsError(client, err)
			} else {
				if blk.Number != curHeight {
					return handler.WrapsError(client, ErrUnexpectedBlockNumber)
				}
				// 如果直接使用 block, err = client.GetBlock(curHeight) 这种方式，
				// 且错误是可以重试的则会 block 变量设置为 nil，从而导致：
				// runtime error: invalid memory address or nil pointer dereference
				block = blk

			}
		}
		return nil
	})
	return
}

func (b *BlockSpider) isForkedBlock(height uint64, block *Block) bool {
	preHeight := height - 1
	preHash := block.ParentHash
	curPreBlockHash, _ := b.store.LoadBlockHash(preHeight)

	//分叉孤块处理
	return curPreBlockHash != "" && curPreBlockHash != preHash
}

func (b *BlockSpider) handleTx(block *Block, chainHeight uint64, handler BlockHandler, opt *getBlocksOpt) (txHandler TxHandler, err error) {
	if err := b.composeTxsInBlock(block, handler, opt); err != nil {
		return nil, err
	}

	err = b.WithRetry(func(client Clienter) (err error) {
		txHandler, err = handler.OnNewBlock(client, chainHeight, block)
		return handler.WrapsError(client, err)
	})

	if err != nil {
		return nil, err
	}

	concurrency := b.concurrencyTxs
	if concurrency > len(block.Transactions) {
		concurrency = len(block.Transactions)
	}
	if concurrency > 1 {
		jobsChan := make(chan *Transaction, len(block.Transactions)+concurrency)
		wg := &sync.WaitGroup{}
		lock := sync.Mutex{}
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for tx := range jobsChan {
					if tx == nil {
						// stop
						break
					}

					innerErr := b.WithRetry(func(client Clienter) error {
						return handler.WrapsError(client, txHandler.OnNewTx(client, block, tx))
					})
					if innerErr != nil {
						lock.Lock()
						err = innerErr
						lock.Unlock()
					}
				}
			}()
		}

		for _, tx := range block.Transactions {
			// NOTE: nil tx may dropped on-chain.
			// See handler.IsDroppedTx invocation in composeTxsInBlock
			if tx != nil {
				jobsChan <- tx
			}
		}
		for i := 0; i < concurrency; i++ {
			jobsChan <- nil // stop
		}
		wg.Wait()
		close(jobsChan) // close inner tx
		return
	}

	for _, tx := range block.Transactions {
		err = b.WithRetry(func(client Clienter) error {
			return handler.WrapsError(client, txHandler.OnNewTx(client, block, tx))
		})
		if err != nil {
			return nil, err
		}
	}
	return
}

// composeTxsInBlock use `GetTxByHash` to fill txs in block,
// if the Raw pointer of tx is nil.
func (b *BlockSpider) composeTxsInBlock(block *Block, handler BlockHandler, opt *getBlocksOpt) (err error) {
	neededIndices := make([]int, 0, len(block.Transactions))
	for i, tx := range block.Transactions {
		if tx.Raw == nil && tx.Hash != "" {
			neededIndices = append(neededIndices, i)
		}
	}
	if len(neededIndices) == 0 {
		return nil
	}

	concurrency := opt.concurrency()
	if concurrency > len(neededIndices) {
		concurrency = len(neededIndices)
	}

	jobsChan := make(chan int, len(neededIndices)+concurrency)
	wg := &sync.WaitGroup{}
	lock := &sync.RWMutex{}

	// start workers.
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobsChan {
				if idx == -1 {
					// stop
					break
				}
				lock.RLock()
				if err != nil {
					return
				}
				lock.RUnlock()

				txHash := block.Transactions[idx].Hash

				innerErr := b.WithRetry(func(client Clienter) error {
					newTx, err := client.GetTxByHash(txHash)
					if err != nil {
						return handler.WrapsError(client, err)
					}
					block.Transactions[idx] = newTx
					return nil
				})

				if handler.IsDroppedTx(block.Transactions[idx], innerErr) {
					continue
				}

				if innerErr != nil {
					lock.Lock()
					err = innerErr
					lock.Unlock()
				}
			}
		}()
	}

	for _, i := range neededIndices {
		jobsChan <- i
	}
	for i := 0; i < concurrency; i++ {
		jobsChan <- -1 // stop
	}

	wg.Wait()
	close(jobsChan) // close
	return
}

func (b *BlockSpider) warnSlow(handler BlockHandler, block *Block) {
	if len(block.Metrics) == 0 {
		return
	}
	begin := block.Metrics[0].Begin
	end := block.Metrics[len(block.Metrics)-1].End
	thr := 3 * time.Second
	if end.Sub(begin) < thr {
		return
	}

	builder := strings.Builder{}
	var prevMetric *Metric
	for _, m := range block.Metrics {
		if prevMetric != nil {
			builder.WriteString(fmt.Sprintf(" -- %dms --> ", m.Begin.Sub(prevMetric.End)/time.Millisecond))
		}
		builder.WriteString(fmt.Sprintf("%s (%dms)", m.Stage, m.Elapsed()/time.Millisecond))
		prevMetric = m
	}
	handler.OnError(
		fmt.Errorf("[total elapsed %dms: %s] %w", end.Sub(begin)/time.Millisecond, builder.String(), ErrSlowBlockHandling),
		HeightInfo{
			CurHeight: block.Number,
		},
	)
}

// SealPendingTransactions load pending transactions and try to seal them.
func (b *BlockSpider) SealPendingTransactions(handler BlockHandler) {
	if err := b.doSealPendingTxs(handler); err != nil {
		handler.OnError(err)
	}
}

func (b *BlockSpider) doSealPendingTxs(handler BlockHandler) error {
	txs, err := b.store.LoadPendingTxs()
	if err != nil {
		return err
	}

	for _, tx := range txs {
		var txHandler TxHandler

		err = b.WithRetry(func(client Clienter) (err error) {
			txHandler, err = handler.CreateTxHandler(client, tx)
			return handler.WrapsError(client, err)
		})

		if err != nil {
			handler.OnError(err)
			continue
		}
		if err := b.sealOnePendingTx(handler, txHandler, tx); err != nil {
			handler.OnError(err)
			continue
		}

		err = b.WithRetry(func(client Clienter) error {
			err := txHandler.Save(client)
			return handler.WrapsError(client, err)
		})
		if err != nil {
			handler.OnError(err)
			continue
		}

		b.accumulator.accumulateTx(tx)
	}
	b.monitorContinuousSentTxFailure()
	return nil
}

func (b *BlockSpider) sealOnePendingTx(handler BlockHandler, txHandler TxHandler, tx *Transaction) error {
	var txByHash *Transaction
	err := b.WithRetry(func(client Clienter) error {
		if p, ok := b.prefetchedTxs[tx.Hash]; ok {
			txByHash = p
			return nil
		}

		var err error
		txByHash, err = client.GetTxByHash(tx.Hash)
		return handler.WrapsError(client, err)
	})

	if handler.IsDroppedTx(txByHash, err) {
		err = b.WithRetry(func(client Clienter) error {
			err := txHandler.OnDroppedTx(client, tx)
			return handler.WrapsError(client, err)
		})

		if err != nil {
			return err
		}
		return nil
	}

	if err != nil {
		return err
	}

	txByHash.Record = tx.Record

	err = b.WithRetry(func(client Clienter) error {
		err := txHandler.OnSealedTx(client, txByHash)
		return handler.WrapsError(client, err)
	})
	return err
}

// WithRetry retry.
func (b *BlockSpider) WithRetry(fn func(client Clienter) error) error {
	err := b.detector().WithRetry(b.detector().Len(), func(node detector.Node) error {
		return fn(node.(Clienter))
	})
	if err != nil {
		if r, ok := err.(*RetryStandbyErr); ok {
			if b.standby.Len() == 0 {
				return r.inner
			}
			err = b.standby.WithRetry(b.standby.Len(), func(node detector.Node) error {
				return fn(node.(Clienter))
			})
		}
	}
	if err == detector.ErrRingEmpty {
		time.Sleep(time.Second * 3)
	}
	return common.UnwrapRetryErr(err)
}

// monitorContinuousSentTxFailure check if have continuous tx failed
// that sent by us.
func (b *BlockSpider) monitorContinuousSentTxFailure() {
	for txType, store := range b.accumulator.getResults() {
		txs := store.getResult()
		if len(txs) > 0 {
			for _, w := range b.watchers {
				w.OnSentTxFailed(txType, len(txs), txs)
			}
		}
	}
}

// RetryStandbyErr wraps error to retry on standby nodes.
type RetryStandbyErr struct {
	inner error
}

func (r *RetryStandbyErr) Error() string {
	return r.inner.Error()
}

// RetryStandby use standby node to retry.
func RetryStandby(err error) error {
	if err == nil {
		return nil
	}
	return &RetryStandbyErr{
		inner: err,
	}
}

// ForkDeltaOverflow delta between current chain height and
// forked block number is greater than safety.
type ForkDeltaOverflow struct {
	ChainHeight uint64
	BlockNumber uint64
	SafelyDelta uint64
}

func (*ForkDeltaOverflow) Error() string {
	return "delta between chain height and forked block number is greater than safety"
}
