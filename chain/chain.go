package chain

import (
	"errors"
	"math"
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
	GetTxByHash(txHash string) (tx interface{}, isPending bool, err error)
}

// StateStore to store block height.
type StateStore interface {
	// StoreHeight store height to crawl.
	StoreHeight(height uint64) error

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
	IsPending   bool
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

	OnBlock(client Clienter, block *Block) (TxHandler, error)

	OnForkedBlock(client Clienter, block *Block) error
	OnError(height uint64, err error)
}

// TxHandler to handle txs.
type TxHandler interface {
	OnNewTx(client Clienter, block *Block, tx *Transaction) error
	OnDroppedTx(client Clienter, block *Block, tx *Transaction) error
	OnSealedTx(client Clienter, block *Block, tx *Transaction) error
	Save(client Clienter) error
}

// BlockSpider block spider to crawl blocks from chain.
type BlockSpider struct {
	detector detector.Detector
	store    StateStore
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

func (b *BlockSpider) Start(handler BlockHandler) {
	go b.StartIndexBlock(handler)
}

// StartIndexBlock starts a loop to index blocks: extract transactions from block.
func (b *BlockSpider) StartIndexBlock(handler BlockHandler) {
	b.detector.DetectAll()
	b.detector.StartDetectPlan(3*time.Minute, 0, 3*time.Minute)
	b.detector.StartDetectPlan(10*time.Minute, 3*time.Minute, 10*time.Minute)
	b.detector.StartDetectPlan(20*time.Minute, 10*time.Minute, math.MaxInt64)

	for {
		height, curHeight, err := b.getHeights()
		if err != nil {
			handler.OnError(height, err)
			continue
		}
		if curHeight > height {
			time.Sleep(handler.BlockInterval())
			continue
		}

		block, curHeight, err := b.getBlock(curHeight, handler)
		if err != nil {
			handler.OnError(curHeight, err)
			continue
		}

		var txHandler TxHandler

		err = b.withRetry(func(client Clienter) (err error) {

			// 保存对应块高hash
			if err := b.store.StoreBlockHash(curHeight, block.Hash); err != nil {
				return err
			}

			txHandler, err = handler.OnBlock(client, block)
			return err
		})

		if err != nil {
			handler.OnError(curHeight, err)
			continue
		}

		for _, tx := range block.Transactions {
			err = b.withRetry(func(client Clienter) error {
				return txHandler.OnNewTx(client, block, tx)
			})
			if err != nil {
				handler.OnError(curHeight, err)
			}
		}

		err = b.withRetry(func(client Clienter) error {
			if err := txHandler.Save(client); err != nil {
				return err
			}
			return b.store.StoreHeight(curHeight + 1)

		})
		if err != nil {
			handler.OnError(curHeight, err)
		}
	}
}

func (b *BlockSpider) getHeights() (height, curHeight uint64, err error) {
	err = b.withRetry(func(client Clienter) error {
		height, err = client.GetBlockHeight()
		if err != nil {
			return err
		}
		curHeight, err = b.store.LoadHeight()
		if err == ErrNoCurrentHeight {
			curHeight = height
		} else if err != nil {
			return err
		}
		return nil
	})
	return
}

func (b *BlockSpider) getBlock(height uint64, handler BlockHandler) (block *Block, curHeight uint64, err error) {
	curHeight = height
	err = b.withRetry(func(client Clienter) error {
		// TODO: concurrent(remove forked block may block concurrent).
		block, err = client.GetBlock(curHeight)
		if err != nil {
			return err
		}

		if handler.BlockMayFork() {
			for b.isForkedBlock(curHeight, block) {
				curHeight = curHeight - 1

				if err := handler.OnForkedBlock(client, block); err != nil {
					return err
				}

				block, err = client.GetBlock(curHeight)
				if err != nil {
					return err
				}
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

// SealPendingTransactions load pending transactions and try to seal them.
func (b *BlockSpider) SealPendingTransactions(handler BlockHandler) {
	txs, err := b.store.LoadPendingTxs()
	if err != nil {
		handler.OnError(0, err)
	}
	blocks := make(map[uint64]*Block)

	for _, tx := range txs {
		var txByHash interface{}
		var isPending bool
		err := b.withRetry(func(client Clienter) error {
			var err error
			txByHash, isPending, err = client.GetTxByHash(tx.Hash)
			return err
		})
		if err != nil {
			handler.OnError(tx.BlockNumber, err)
			continue
		}
		if isPending {
			continue
		}

		var block *Block
		if blk, ok := blocks[tx.BlockNumber]; ok {
			block = blk
		} else {
			err = b.withRetry(func(client Clienter) error {
				var err error
				block, err = client.GetBlock(tx.BlockNumber)
				return err
			})
			if err != nil {
				handler.OnError(tx.BlockNumber, err)
			}
			blocks[tx.BlockNumber] = block
		}

		var txHandler TxHandler

		err = b.withRetry(func(client Clienter) (err error) {
			txHandler, err = handler.OnBlock(client, block)
			return
		})
		if err != nil {
			handler.OnError(tx.BlockNumber, err)
			continue
		}

		if txByHash == nil {
			err = b.withRetry(func(client Clienter) error {
				return txHandler.OnDroppedTx(client, block, tx)
			})

			if err != nil {
				handler.OnError(tx.BlockNumber, err)
			}
			continue
		}

		err = b.withRetry(func(client Clienter) error {
			for _, blkTx := range block.Transactions {
				if tx.Hash == blkTx.Hash {
					blkTx.Record = tx.Record
					return txHandler.OnSealedTx(client, block, blkTx)
				}
			}
			return nil
		})
		if err != nil {
			handler.OnError(tx.BlockNumber, err)
			continue
		}

		err = b.withRetry(func(client Clienter) error {
			return txHandler.Save(client)
		})
		if err != nil {
			handler.OnError(tx.BlockNumber, err)
		}
	}
}

func (b *BlockSpider) withRetry(fn func(client Clienter) error) error {
	return b.detector.WithRetry(b.detector.Len(), func(node detector.Node) error {
		return fn(node.(Clienter))
	})
}
