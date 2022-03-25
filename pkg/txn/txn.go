package txn

import "sync"

type Transaction struct {
	sync.WaitGroup
	Mgr   *TxnManager
	Store *Store
	Ctx   *TxnCtx
	Err   error
}

func NewTxn(mgr *TxnManager, txnId uint64, start uint64, info []byte) *Transaction {
	return &Transaction{
		Mgr:   mgr,
		Store: NewStore(),
		Ctx: &TxnCtx{
			ID:       txnId,
			StartTS:  start,
			CommitTS: UncommitTS,
			Info:     info,
		},
	}
}

func (txn *Transaction) IsTerminated() bool {
	return txn.Ctx.IsTerminated()
}

// TODO: just a demo
func (txn *Transaction) Commit() error {
	txn.Add(1)
	txn.Mgr.OnCommitTxn(txn)
	txn.Wait()
	return txn.Err
}
