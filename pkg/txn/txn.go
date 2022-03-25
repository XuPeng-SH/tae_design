package txn

import (
	"sync"
)

type OpType int8

const (
	OpCommit = iota
	OpRollback
)

type OpTxn struct {
	Txn *Transaction
	Op  OpType
}

type Transaction struct {
	sync.RWMutex
	sync.WaitGroup
	Mgr             *TxnManager
	Store           *Store
	Ctx             *TxnCtx
	Err             error
	DoneCond        sync.Cond
	PrepareCommitFn func(*Transaction) error
}

func NewTxn(mgr *TxnManager, txnId uint64, start uint64, info []byte) *Transaction {
	txn := &Transaction{
		Mgr:   mgr,
		Store: NewStore(),
	}
	txn.Ctx = NewTxnCtx(&txn.RWMutex, txnId, start, info)
	txn.DoneCond = *sync.NewCond(txn)
	return txn
}

func (txn *Transaction) Commit() error {
	txn.Add(1)
	txn.Mgr.OnOpTxn(&OpTxn{
		Txn: txn,
		Op:  OpCommit,
	})
	txn.Wait()
	return txn.Err
}

func (txn *Transaction) Rollback() error {
	txn.Add(1)
	txn.Mgr.OnOpTxn(&OpTxn{
		Txn: txn,
		Op:  OpRollback,
	})
	txn.Wait()
	return txn.Err
}

func (txn *Transaction) Done() {
	txn.DoneCond.L.Lock()
	txn.Ctx.ToCommittedLocked()
	txn.WaitGroup.Done()
	txn.DoneCond.Broadcast()
	txn.DoneCond.L.Unlock()
}

func (txn *Transaction) WaitIfCommitting() {
	txn.RLock()
	if txn.Ctx.State != TxnStateCommitting {
		txn.RUnlock()
		return
	}
	txn.RUnlock()
	txn.DoneCond.L.Lock()
	if txn.Ctx.State != TxnStateCommitting {
		txn.DoneCond.L.Unlock()
		return
	}
	txn.DoneCond.Wait()
	txn.DoneCond.L.Unlock()
}

func (txn *Transaction) PreapreCommit() error {
	return nil
}

func (txn *Transaction) PreapreRollback() error {
	return nil
}
