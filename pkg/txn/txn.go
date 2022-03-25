package txn

import "sync"

type Transaction struct {
	sync.RWMutex
	sync.WaitGroup
	Mgr   *TxnManager
	Store *Store
	Ctx   *TxnCtx
	Err   error
}

func NewTxn(mgr *TxnManager, txnId uint64, start uint64, info []byte) *Transaction {
	txn := &Transaction{
		Mgr:   mgr,
		Store: NewStore(),
	}
	txn.Ctx = NewTxnCtx(&txn.RWMutex, txnId, start, info)
	return txn
}

// func (txn *Transaction) IsTerminated() bool {
// 	return txn.Ctx.IsTerminated()
// }

// TODO: just a demo
func (txn *Transaction) Commit() error {
	txn.Add(1)
	txn.Mgr.OnCommitTxn(txn)
	txn.Wait()
	return txn.Err
}
