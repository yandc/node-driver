package chain

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"gitlab.bixin.com/mili/node-driver/detector"
)

var (
	// ErrNoCurrentHeight can't load height from cache or DB.
	ErrNoCurrentHeight = errors.New("we don't start yet, go from the newest height")

	// ErrSlowBlockHandling slow block handling
	ErrSlowBlockHandling = errors.New("slow block handling")
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

	// GetBalance fetch balance of major coin.
	// GetBalance(addr string) (interface{}, error)

	// GetTokensBalance fetch balance of tokens.
	// GetTokensBalance(addr string, tokens []string) (map[string]interface{}, error)

	// GetBlock fetch block data of the given height.
	GetBlock(height uint64) (*Block, error)

	// GetBlockHeight get current block height.
	GetBlockHeight() (uint64, error)

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

	Raw    interface{}
	Record interface{}
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

// BlockSpider block spider to crawl blocks from chain.
type BlockSpider struct {
	detector detector.Detector
	standby  detector.Detector
	store    StateStore
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
		detector: d,
		store:    store,
		standby:  detector.NewSimpleDetector(),
	}
}

func (b *BlockSpider) EnableRoundRobin() {
	b.detector.EnableRoundRobin()
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

func (b *BlockSpider) WatchDetector(watchers ...DetectorWatcher) {
	for _, w := range watchers {
		b.detector.Watch(w.OnNodesChange)
		b.detector.WatchFailover(w.OnNodeFailover)
		b.detector.WatchSuccess(w.OnNodeSuccess)
	}
}

// StartIndexBlock starts a loop to index blocks: extract transactions from block.
//
// - concurrentDeltaThr indicates how many blocks delta from chain is safe to get blocks concurrently.
// - maxConcurrency indicates the max goroutines to get blocks concurrently.
func (b *BlockSpider) StartIndexBlock(handler BlockHandler, concurrentDeltaThr int, maxConcurrency int) {
	b.detector.DetectAll()
	go b.detector.StartDetectPlan(3*time.Minute, 0, 3*time.Minute)
	go b.detector.StartDetectPlan(10*time.Minute, 3*time.Minute, 10*time.Minute)
	go b.detector.StartDetectPlan(20*time.Minute, 10*time.Minute, math.MaxInt64)

	for {
	_START:
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
				goto _START
			}

			if err := b.store.StoreHeight(opt.localHeight + uint64(indexedBlockNum)); err != nil {
				handler.OnError(err, HeightInfo{
					ChainHeight: opt.chainHeight,
					CurHeight:   opt.localHeight,
				})
				goto _START
			}
			curHeight += uint64(indexedBlockNum)
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
			txHandler, err := b.handleTx(block, chainHeight, handler)
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
		block, newHeight, err = b.handleBlockMayFork(opt.localHeight, handler, block)
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
	nodeURLs := make([]string, 0, b.detector.Len())
	err = b.WithRetry(func(client Clienter) error {
		nodeURLs = append(nodeURLs, client.URL())
		block, err = client.GetBlock(height)
		return handler.WrapsError(client, err)
	})
	if block != nil {
		block.Metrics = append(block.Metrics, &Metric{
			Stage:    "retrieveBlock",
			NodeURLs: nodeURLs,
			Begin:    start,
			End:      time.Now(),
		})
	}
	return
}

func (b *BlockSpider) handleBlockMayFork(height uint64, handler BlockHandler, inBlock *Block) (block *Block, curHeight uint64, err error) {
	block = inBlock
	curHeight = height

	err = b.WithRetry(func(client Clienter) error {
		for b.isForkedBlock(curHeight, block) {
			curHeight = curHeight - 1

			if err := handler.OnForkedBlock(client, block); err != nil {
				return handler.WrapsError(client, err)
			}
			// TODO: Store height.
			if blk, err := client.GetBlock(curHeight); err != nil {
				return handler.WrapsError(client, err)
			} else {
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

func (b *BlockSpider) handleTx(block *Block, chainHeight uint64, handler BlockHandler) (txHandler TxHandler, err error) {
	err = b.WithRetry(func(client Clienter) (err error) {
		txHandler, err = handler.OnNewBlock(client, chainHeight, block)
		return handler.WrapsError(client, err)
	})

	if err != nil {
		return nil, err
	}

	for _, tx := range block.Transactions {
		err = b.WithRetry(func(client Clienter) error {
			return txHandler.OnNewTx(client, block, tx)
		})
		if err != nil {
			return nil, err
		}
	}
	return
}

func (b *BlockSpider) warnSlow(handler BlockHandler, block *Block) {
	if len(block.Metrics) == 0 {
		return
	}
	begin := block.Metrics[0].Begin
	end := block.Metrics[len(block.Metrics)-1].End
	thr := 5 * time.Second
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
	}
	return nil
}

func (b *BlockSpider) sealOnePendingTx(handler BlockHandler, txHandler TxHandler, tx *Transaction) error {
	var txByHash *Transaction
	err := b.WithRetry(func(client Clienter) error {
		var err error
		txByHash, err = client.GetTxByHash(tx.Hash)
		return handler.WrapsError(client, err)
	})

	if err != nil {
		return err
	}

	if txByHash == nil {
		err = b.WithRetry(func(client Clienter) error {
			err := txHandler.OnDroppedTx(client, tx)
			return handler.WrapsError(client, err)
		})

		if err != nil {
			return err
		}
		return nil
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
	err := b.detector.WithRetry(b.detector.Len(), func(node detector.Node) error {
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
	return err
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
