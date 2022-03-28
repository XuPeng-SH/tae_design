package txn

import (
	"sync"
	"tae/pkg/iface"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore/sm"
	"github.com/sirupsen/logrus"
)

type TxnStoreFactory = func() TxnStore

type TxnManager struct {
	sync.RWMutex
	sm.ClosedState
	sm.StateMachine
	Active           map[uint64]iface.AsyncTxn
	IdAlloc, TsAlloc *common.IdAlloctor
	TxnStoreFactory  TxnStoreFactory
}

func NewTxnManager(txnStoreFactory TxnStoreFactory) *TxnManager {
	mgr := &TxnManager{
		Active:          make(map[uint64]iface.AsyncTxn),
		IdAlloc:         common.NewIdAlloctor(1),
		TsAlloc:         common.NewIdAlloctor(1),
		TxnStoreFactory: txnStoreFactory,
	}
	pqueue := sm.NewSafeQueue(10000, 200, mgr.onPreparing)
	cqueue := sm.NewSafeQueue(10000, 200, mgr.onCommit)
	mgr.StateMachine = sm.NewStateMachine(new(sync.WaitGroup), mgr, pqueue, cqueue)
	return mgr
}

func (mgr *TxnManager) Init(prevTxnId uint64, prevTs uint64) error {
	mgr.IdAlloc.SetStart(prevTxnId)
	mgr.TsAlloc.SetStart(prevTs)
	return nil
}

func (mgr *TxnManager) StartTxn(info []byte) iface.AsyncTxn {
	mgr.Lock()
	defer mgr.Unlock()
	txnId := mgr.IdAlloc.Alloc()
	startTs := mgr.TsAlloc.Alloc()

	store := mgr.TxnStoreFactory()
	txn := NewTxn(mgr, store, txnId, startTs, info)
	mgr.Active[txnId] = txn
	return txn
}

func (mgr *TxnManager) GetTxn(id uint64) iface.AsyncTxn {
	mgr.RLock()
	defer mgr.RUnlock()
	return mgr.Active[id]
}

func (mgr *TxnManager) OnOpTxn(op *OpTxn) {
	mgr.EnqueueRecevied(op)
}

func (mgr *TxnManager) onPreparCommit(txn iface.AsyncTxn) {
	txn.SetError(txn.PreapreCommit())
}

func (mgr *TxnManager) onPreparRollback(txn iface.AsyncTxn) {
	txn.SetError(txn.PreapreRollback())
}

// TODO
func (mgr *TxnManager) onPreparing(items ...interface{}) {
	for _, item := range items {
		op := item.(*OpTxn)
		ts := mgr.TsAlloc.Alloc()
		mgr.Lock()
		op.Txn.Lock()
		if op.Op == OpCommit {
			op.Txn.ToCommittingLocked(ts)
		} else if op.Op == OpRollback {
			op.Txn.ToRollbackingLocked(ts)
		}
		op.Txn.Unlock()
		mgr.Unlock()
		if op.Op == OpCommit {
			mgr.onPreparCommit(op.Txn)
			if op.Txn.GetError() != nil {
				op.Op = OpRollback
				op.Txn.Lock()
				op.Txn.ToRollbackingLocked(ts)
				op.Txn.Unlock()
				mgr.onPreparRollback(op.Txn)
			}
		} else {
			mgr.onPreparRollback(op.Txn)
		}
		mgr.EnqueueCheckpoint(op)
	}
}

// TODO
func (mgr *TxnManager) onCommit(items ...interface{}) {
	for _, item := range items {
		op := item.(*OpTxn)
		op.Txn.WaitDone()
		logrus.Infof("%s Done", op.Repr())
	}
}
