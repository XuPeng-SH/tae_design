package txnbase

import (
	"fmt"
	"sync"
	"tae/pkg/iface/txnif"

	"github.com/sirupsen/logrus"
)

type OpType int8

const (
	OpCommit = iota
	OpRollback
)

type OpTxn struct {
	Txn txnif.AsyncTxn
	Op  OpType
}

func (txn *OpTxn) Repr() string {
	if txn.Op == OpCommit {
		return fmt.Sprintf("[Commit][Txn-%d]", txn.Txn.GetID())
	} else {
		return fmt.Sprintf("[Rollback][Txn-%d]", txn.Txn.GetID())
	}
}

type transaction struct {
	sync.RWMutex
	sync.WaitGroup
	*TxnCtx
	Mgr             *TxnManager
	txnStore        txnif.TxnStore
	Err             error
	DoneCond        sync.Cond
	PrepareCommitFn func(interface{}) error
}

func NewTxn(mgr *TxnManager, store txnif.TxnStore, txnId uint64, start uint64, info []byte) *transaction {
	txn := &transaction{
		Mgr:      mgr,
		txnStore: store,
	}
	txn.TxnCtx = NewTxnCtx(&txn.RWMutex, txnId, start, info)
	txn.DoneCond = *sync.NewCond(txn)
	return txn
}

func (txn *transaction) SetError(err error) { txn.Err = err }
func (txn *transaction) GetError() error    { return txn.Err }

func (txn *transaction) SetPrepareCommitFn(fn func(interface{}) error) { txn.PrepareCommitFn = fn }

func (txn *transaction) Commit() error {
	txn.Add(1)
	txn.Mgr.OnOpTxn(&OpTxn{
		Txn: txn,
		Op:  OpCommit,
	})
	txn.Wait()
	return txn.Err
}

func (txn *transaction) GetStore() txnif.TxnStore {
	return txn.txnStore
}

func (txn *transaction) Rollback() error {
	txn.Add(1)
	txn.Mgr.OnOpTxn(&OpTxn{
		Txn: txn,
		Op:  OpRollback,
	})
	txn.Wait()
	return txn.Err
}

func (txn *transaction) Done() {
	txn.DoneCond.L.Lock()
	txn.ToCommittedLocked()
	txn.WaitGroup.Done()
	txn.DoneCond.Broadcast()
	txn.DoneCond.L.Unlock()
}

func (txn *transaction) IsTerminated(waitIfcommitting bool) bool {
	state := txn.GetTxnState(waitIfcommitting)
	return state == txnif.TxnStateCommitted || state == txnif.TxnStateRollbacked
}

func (txn *transaction) GetTxnState(waitIfcommitting bool) int32 {
	txn.RLock()
	state := txn.State
	if !waitIfcommitting {
		txn.RUnlock()
		return state
	}
	if state != txnif.TxnStateCommitting {
		txn.RUnlock()
		return state
	}
	txn.RUnlock()
	txn.DoneCond.L.Lock()
	state = txn.State
	if state != txnif.TxnStateCommitting {
		txn.DoneCond.L.Unlock()
		return state
	}
	txn.DoneCond.Wait()
	txn.DoneCond.L.Unlock()
	return state
}

func (txn *transaction) PreapreCommit() error {
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

func (txn *transaction) PreapreRollback() error {
	logrus.Infof("Prepare Rollbacking %d", txn.ID)
	return nil
}

func (txn *transaction) WaitDone() error {
	// TODO
	logrus.Infof("Wait txn %d done", txn.ID)
	txn.Done()
	return txn.Err
}
