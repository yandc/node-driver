package chain

import (
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	"gitlab.bixin.com/mili/node-driver/detector"
)

var (
	// ErrNoCurrentHeight can't load height from cache or DB.
	ErrNoCurrentHeight = errors.New("we don't start yet, go from the newest height")
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

	Transactions []*Transaction
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

// BlockHandler to handle block.
type BlockHandler interface {
	// BlockInterval returns how long the system take to produce a block.
	BlockInterval() time.Duration

	// BlockMayFork returns true if the block may be forked.
	BlockMayFork() bool

	// OnNewBlock will be invoked when crawl new block.
	OnNewBlock(client Clienter, block *Block) (TxHandler, error)

	// CreateTxHandler create TxHandler directly without a block.
	// Will be invoked when seal pending txs and tx is not on the chain.
	CreateTxHandler(client Clienter, tx *Transaction) (TxHandler, error)

	// OnForkedBlock will be invoked when determine block has been forked.
	OnForkedBlock(client Clienter, block *Block) error

	// OnError will be invoked when error occurred.
	// Returns true will ignore current block.
	OnError(err error) (incrHeight bool)

	// WrapsError wraps error to control when to retry.
	WrapsError(err error) error
}

// TxHandler to handle txs.
type TxHandler interface {
	// OnNewTx will be invoked when retrieved new tx from block.
	OnNewTx(client Clienter, block *Block, tx *Transaction) error

	// OnDroppedTx will be invoked when Clienter.GetTxByHash returns nil without error.
	OnDroppedTx(client Clienter, block *Block, tx *Transaction) error

	// OnSealedTx will be invoked when Clienter.GetTxByHash returns non-nil without error.
	OnSealedTx(client Clienter, block *Block, tx *Transaction, txByHashRaw interface{}) error

	// Save will be invoked after muliple invocations of above.
	// WILL NOT INVOKE CONCURRENTLY.
	Save(client Clienter) error
}

// DetectorWatcher to watch nodes changed.
type DetectorWatcher interface {
	OnNodesChange([]detector.Node)
	OnNodeFailover(current detector.Node, next detector.Node)
}

// BlockSpider block spider to crawl blocks from chain.
type BlockSpider struct {
	detector detector.Detector
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

	return opt.heightDelta() >= opt.concurrentDeltaThr
}

func (opt getBlocksOpt) concurrency() int {
	if opt.maxConcurrency <= 0 {
		return 1
	}

	concurrency := opt.heightDelta()
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
	}
}

func (b *BlockSpider) Start(handler BlockHandler, concurrentDeltaThr int, maxConcurrency int) {
	go b.StartIndexBlock(handler, concurrentDeltaThr, maxConcurrency)
}

func (b *BlockSpider) WatchDetector(watchers ...DetectorWatcher) {
	for _, w := range watchers {
		b.detector.Watch(w.OnNodesChange)
		b.detector.WatchFailover(w.OnNodeFailover)
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

		opt := getBlocksOpt{
			localHeight:        curHeight,
			chainHeight:        height,
			concurrentDeltaThr: concurrentDeltaThr,
			maxConcurrency:     maxConcurrency,
		}

		indexedBlockNum, storeHeight := b.doIndexBlocks(handler, &opt)

		if !storeHeight {
			continue
		}

		if err := b.store.StoreHeight(opt.localHeight + uint64(indexedBlockNum)); err != nil {
			handler.OnError(err)
			continue
		}
	}
}

func (b *BlockSpider) handleErrs(handler BlockHandler, errs []error) (storeHeight bool) {
	storeHeight = true
	for _, err := range errs {
		storeHeight = storeHeight && handler.OnError(err)
	}
	return storeHeight
}

func (b *BlockSpider) doIndexBlocks(handler BlockHandler, opt *getBlocksOpt) (indexedBlockNum int, storeHeight bool) {
	blocks, errs := b.getBlocks(opt, handler)

	if !b.handleErrs(handler, errs) {
		return 0, false
	}
	indexedBlockNum = len(blocks) + len(errs)

	errs = make([]error, 0, len(blocks))

	txHandlers := make([]*blockTxHandler, 0, len(blocks))
	var (
		wg   sync.WaitGroup
		lock sync.Mutex
	)

	for _, blk := range blocks {
		wg.Add(1)
		go func(block *Block) {
			defer wg.Done()
			txHandler, err := b.handleTx(block, handler)

			lock.Lock()
			defer lock.Unlock()

			if err != nil {
				errs = append(errs, err)
				return
			}

			txHandlers = append(txHandlers, &blockTxHandler{
				block: block,
				inner: txHandler,
			})
		}(blk)
	}

	wg.Wait()

	if !b.handleErrs(handler, errs) {
		return 0, false
	}

	sort.Slice(txHandlers, func(i, j int) bool {
		return txHandlers[i].block.Number < txHandlers[j].block.Number
	})

	var err error
	// XXX Using this style of for-loop to break when err is not nil.
	for i := 0; i < len(txHandlers) && err == nil; i++ {
		txHandler := txHandlers[i]

		err = b.WithRetry(func(client Clienter) error {
			return handler.WrapsError(txHandler.inner.Save(client))
		})
		if err != nil {
			errs = append(errs, err)
		}
	}

	// 保存对应块高 hash
	for _, blk := range blocks {
		if err := b.store.StoreBlockHash(blk.Number, blk.Hash); err != nil {
			errs = append(errs, err)
		}
	}
	if !b.handleErrs(handler, errs) {
		return 0, false
	}
	return indexedBlockNum, true
}

func (b *BlockSpider) getHeights(handler BlockHandler) (height, curHeight uint64, err error) {
	err = b.WithRetry(func(client Clienter) error {
		height, err = client.GetBlockHeight()
		return handler.WrapsError(err)

	})

	curHeight, err = b.store.LoadHeight()
	if err == ErrNoCurrentHeight {
		curHeight = height
		err = nil
	}
	return
}

func (b *BlockSpider) getBlocks(opt *getBlocksOpt, handler BlockHandler) ([]*Block, []error) {
	if opt.safelyConcurrent() {
		return b.concurrentGetBlocks(opt.localHeight, opt.chainHeight, handler, opt.concurrency())
	}
	errs := make([]error, 0, 1)
	block, err := b.getBlock(opt.localHeight, handler)
	if err != nil {
		errs = append(errs, err)
		return nil, errs
	}
	if handler.BlockMayFork() {
		var newHeight uint64
		block, newHeight, err = b.handleBlockMayFork(opt.localHeight, handler, block)
		if err != nil {
			errs = append(errs, err)
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
func (b *BlockSpider) concurrentGetBlocks(curHeight, height uint64, handler BlockHandler, concurrency int) ([]*Block, []error) {
	var wg sync.WaitGroup
	var lock sync.Mutex
	blocks := make([]*Block, 0, concurrency)
	errs := make([]error, 0, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(height uint64) {
			defer wg.Done()
			block, err := b.getBlock(height, handler)

			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				errs = append(errs, err)
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
	err = b.WithRetry(func(client Clienter) error {
		block, err = client.GetBlock(height)
		return handler.WrapsError(err)
	})
	return
}

func (b *BlockSpider) handleBlockMayFork(height uint64, handler BlockHandler, inBlock *Block) (block *Block, curHeight uint64, err error) {
	block = inBlock
	curHeight = height

	err = b.WithRetry(func(client Clienter) error {
		for b.isForkedBlock(curHeight, block) {
			curHeight = curHeight - 1

			if err := handler.OnForkedBlock(client, block); err != nil {
				return handler.WrapsError(err)
			}

			block, err = client.GetBlock(curHeight)
			if err != nil {
				return handler.WrapsError(err)
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

func (b *BlockSpider) handleTx(block *Block, handler BlockHandler) (txHandler TxHandler, err error) {
	err = b.WithRetry(func(client Clienter) (err error) {
		txHandler, err = handler.OnNewBlock(client, block)
		return handler.WrapsError(err)
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

	blocksStore := make(map[uint64]*Block)
	for _, tx := range txs {
		var txHandler TxHandler

		err = b.WithRetry(func(client Clienter) (err error) {
			txHandler, err = handler.CreateTxHandler(client, tx)
			return handler.WrapsError(err)
		})

		if err != nil {
			handler.OnError(err)
			continue
		}
		if err := b.sealOnePendingTx(handler, txHandler, blocksStore, tx); err != nil {
			handler.OnError(err)
			continue
		}

		err = b.WithRetry(func(client Clienter) error {
			err := txHandler.Save(client)
			return handler.WrapsError(err)
		})
		if err != nil {
			handler.OnError(err)
			continue
		}
	}
	return nil
}

func (b *BlockSpider) sealOnePendingTx(handler BlockHandler, txHandler TxHandler, blocksStore map[uint64]*Block, tx *Transaction) error {
	var txByHash *Transaction
	err := b.WithRetry(func(client Clienter) error {
		var err error
		txByHash, err = client.GetTxByHash(tx.Hash)
		return handler.WrapsError(err)
	})

	if err != nil {
		return err
	}
	var block *Block

	if txByHash == nil {
		err = b.WithRetry(func(client Clienter) error {
			err := txHandler.OnDroppedTx(client, block, tx)
			return handler.WrapsError(err)
		})

		if err != nil {
			return err
		}
		return nil
	}

	// Pending txs can be created by wallet, and its block number can be zero.
	// So here we are using block number from chain.
	curHeight := txByHash.BlockNumber

	if blk, ok := blocksStore[curHeight]; ok {
		block = blk
	} else {
		err = b.WithRetry(func(client Clienter) error {
			var err error
			block, err = client.GetBlock(curHeight)
			return handler.WrapsError(err)
		})
		if err != nil {
			return err
		}
		blocksStore[curHeight] = block
	}

	err = b.WithRetry(func(client Clienter) error {
		for _, blkTx := range block.Transactions {
			if tx.Hash == blkTx.Hash {
				blkTx.Record = tx.Record
				err := txHandler.OnSealedTx(client, block, blkTx, txByHash.Raw)
				return handler.WrapsError(err)
			}
		}
		return nil
	})
	return err
}

// WithRetry retry.
func (b *BlockSpider) WithRetry(fn func(client Clienter) error) error {
	i := 0
	var err error
	return b.detector.WithRetry(b.detector.Len(), func(node detector.Node) error {
		i += 1
		// Some error will not retry, so we move retry here before the invocation of fn.
		// So there will be no sleep if the error don't need to retry.
		if err != nil {
			time.Sleep(time.Duration(3*i) * time.Second)
		}
		err = fn(node.(Clienter))
		return err
	})
}
