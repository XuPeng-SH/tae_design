package txn

import (
	"fmt"
	"sync"
	"tae/pkg/iface"

	"github.com/sirupsen/logrus"
)

type OpType int8

const (
	OpCommit = iota
	OpRollback
)

type OpTxn struct {
	Txn iface.AsyncTxn
	Op  OpType
}

func (txn *OpTxn) Repr() string {
	if txn.Op == OpCommit {
		return fmt.Sprintf("[Commit][Txn-%d]", txn.Txn.GetID())
	} else {
		return fmt.Sprintf("[Rollback][Txn-%d]", txn.Txn.GetID())
	}
}

type Transaction struct {
	sync.RWMutex
	sync.WaitGroup
	*TxnCtx
	Mgr             *TxnManager
	Store           *Store
	Err             error
	DoneCond        sync.Cond
	PrepareCommitFn func(interface{}) error
}

func NewTxn(mgr *TxnManager, txnId uint64, start uint64, info []byte) *Transaction {
	txn := &Transaction{
		Mgr:   mgr,
		Store: NewStore(),
	}
	txn.TxnCtx = NewTxnCtx(&txn.RWMutex, txnId, start, info)
	txn.DoneCond = *sync.NewCond(txn)
	return txn
}

func (txn *Transaction) SetError(err error) { txn.Err = err }
func (txn *Transaction) GetError() error    { return txn.Err }

func (txn *Transaction) SetPrepareCommitFn(fn func(interface{}) error) { txn.PrepareCommitFn = fn }

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
	txn.ToCommittedLocked()
	txn.WaitGroup.Done()
	txn.DoneCond.Broadcast()
	txn.DoneCond.L.Unlock()
}

func (txn *Transaction) IsTerminated(waitIfcommitting bool) bool {
	state := txn.GetTxnState(waitIfcommitting)
	return state == iface.TxnStateCommitted || state == iface.TxnStateRollbacked
}

func (txn *Transaction) GetTxnState(waitIfcommitting bool) int32 {
	txn.RLock()
	state := txn.State
	if !waitIfcommitting {
		txn.RUnlock()
		return state
	}
	if state != iface.TxnStateCommitting {
		txn.RUnlock()
		return state
	}
	txn.RUnlock()
	txn.DoneCond.L.Lock()
	state = txn.State
	if state != iface.TxnStateCommitting {
		txn.DoneCond.L.Unlock()
		return state
	}
	txn.DoneCond.Wait()
	txn.DoneCond.L.Unlock()
	return state
}

func (txn *Transaction) PreapreCommit() error {
	logrus.Infof("Prepare Committing %d", txn.ID)
	var err error
	if txn.PrepareCommitFn != nil {
		err = txn.PrepareCommitFn(txn)
	}
	if err != nil {
		return err
	}
	// TODO
	return txn.Err
}

func (txn *Transaction) PreapreRollback() error {
	logrus.Infof("Prepare Rollbacking %d", txn.ID)
	return nil
}

func (txn *Transaction) WaitDone() error {
	// TODO
	logrus.Infof("Wait txn %d done", txn.ID)
	txn.Done()
	return txn.Err
}
