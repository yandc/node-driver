package chain

import (
	"sync"
)

// sentTxFailedAccumulator accumulate for alarming(thread safe).
type sentTxFailedAccumulator struct {
	stores sync.Map
}

func (a *sentTxFailedAccumulator) accumulateBlock(block *Block) {
	for _, tx := range block.Transactions {
		a.accumulateTx(tx)
	}

	for _, tx := range block.ExtraTxs {
		a.accumulateTx(tx)
	}
}

func (a *sentTxFailedAccumulator) accumulateTx(tx *Transaction) {
	store, _ := a.stores.LoadOrStore(tx.TxType, &sentTxFailedStore{})
	store.(*sentTxFailedStore).onTx(tx)
}

func (a *sentTxFailedAccumulator) getResults() map[TxType]*sentTxFailedStore {
	results := make(map[TxType]*sentTxFailedStore)
	a.stores.Range(func(key, value interface{}) bool {
		results[key.(TxType)] = value.(*sentTxFailedStore)
		return true
	})
	return results
}

// sentTxFailedAccumulator accumulate for alarming(thread safe).
type sentTxFailedStore struct {
	lock                    sync.RWMutex
	continuousFailedTxHashs []string
}

func (a *sentTxFailedStore) onTx(tx *Transaction) {
	if tx.Result == nil || !tx.Result.MatchedFrom {
		// -  no result;
		// - or it didn't sent by us;
		return
	}

	a.lock.Lock()
	defer a.lock.Unlock()
	if tx.Result.FailedOnChain {
		a.continuousFailedTxHashs = append(a.continuousFailedTxHashs, tx.Hash)
	} else {
		a.continuousFailedTxHashs = nil
	}
}

func (a *sentTxFailedStore) getResult() []string {
	if len(a.continuousFailedTxHashs) == 0 {
		return nil
	}

	a.lock.RLock()
	a.lock.RUnlock()
	results := make([]string, 0, len(a.continuousFailedTxHashs))
	for _, h := range a.continuousFailedTxHashs {
		results = append(results, h)
	}
	return results
}
