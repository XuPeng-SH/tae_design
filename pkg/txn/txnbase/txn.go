package txnbase

import (
	"fmt"
	"sync"
	"tae/pkg/iface/handle"
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

var DefaultTxnFactory = func(mgr *TxnManager, store txnif.TxnStore, id, startTS uint64, info []byte) txnif.AsyncTxn {
	return NewTxn(mgr, store, id, startTS, info)
}

type Txn struct {
	sync.RWMutex
	sync.WaitGroup
	*TxnCtx
	Mgr             *TxnManager
	Store           txnif.TxnStore
	Err             error
	DoneCond        sync.Cond
	PrepareCommitFn func(interface{}) error
}

func NewTxn(mgr *TxnManager, store txnif.TxnStore, txnId uint64, start uint64, info []byte) *Txn {
	txn := &Txn{
		Mgr:   mgr,
		Store: store,
	}
	txn.TxnCtx = NewTxnCtx(&txn.RWMutex, txnId, start, info)
	txn.DoneCond = *sync.NewCond(txn)
	return txn
}

func (txn *Txn) SetError(err error) { txn.Err = err }
func (txn *Txn) GetError() error    { return txn.Err }

func (txn *Txn) SetPrepareCommitFn(fn func(interface{}) error) { txn.PrepareCommitFn = fn }

func (txn *Txn) Commit() error {
	txn.Add(1)
	txn.Mgr.OnOpTxn(&OpTxn{
		Txn: txn,
		Op:  OpCommit,
	})
	txn.Wait()
	return txn.Err
}

func (txn *Txn) GetStore() txnif.TxnStore {
	return txn.Store
}

func (txn *Txn) Rollback() error {
	txn.Add(1)
	txn.Mgr.OnOpTxn(&OpTxn{
		Txn: txn,
		Op:  OpRollback,
	})
	txn.Wait()
	return txn.Err
}

func (txn *Txn) Done() {
	txn.DoneCond.L.Lock()
	txn.ToCommittedLocked()
	txn.WaitGroup.Done()
	txn.DoneCond.Broadcast()
	txn.DoneCond.L.Unlock()
}

func (txn *Txn) IsTerminated(waitIfcommitting bool) bool {
	state := txn.GetTxnState(waitIfcommitting)
	return state == txnif.TxnStateCommitted || state == txnif.TxnStateRollbacked
}

func (txn *Txn) GetTxnState(waitIfcommitting bool) int32 {
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

func (txn *Txn) PrepareCommit() error {
	logrus.Debugf("Prepare Committing %d", txn.ID)
	var err error
	if txn.PrepareCommitFn != nil {
		err = txn.PrepareCommitFn(txn)
	}
	if err != nil {
		return err
	}
	// TODO: process data in store
	err = txn.Store.PrepareCommit()
	return txn.Err
}

func (txn *Txn) DoCommit() error {
	return txn.Store.Commit()
}

func (txn *Txn) PrepareRollback() error {
	logrus.Debugf("Prepare Rollbacking %d", txn.ID)
	return nil
}

func (txn *Txn) WaitDone() error {
	// TODO
	// logrus.Infof("Wait %s Done", txn.String())
	txn.Done()
	return txn.Err
}

func (txn *Txn) CreateDatabase(name string) (db handle.Database, err error) {
	return
}

func (txn *Txn) DropDatabase(name string) (db handle.Database, err error) {
	return
}

func (txn *Txn) GetDatabase(name string) (db handle.Database, err error) {
	return
}
