package txn

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore/sm"
	"github.com/sirupsen/logrus"
)

type TxnManager struct {
	sync.RWMutex
	sm.ClosedState
	sm.StateMachine
	Active           map[uint64]*Transaction
	IdAlloc, TsAlloc *common.IdAlloctor
}

func NewTxnManager() *TxnManager {
	mgr := &TxnManager{
		Active:  make(map[uint64]*Transaction),
		IdAlloc: common.NewIdAlloctor(1),
		TsAlloc: common.NewIdAlloctor(1),
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

func (mgr *TxnManager) StartTxn(info []byte) *Transaction {
	mgr.Lock()
	defer mgr.Unlock()
	txnId := mgr.IdAlloc.Alloc()
	startTs := mgr.TsAlloc.Alloc()

	txn := NewTxn(mgr, txnId, startTs, info)
	mgr.Active[txnId] = txn
	return txn
}

func (mgr *TxnManager) OnOpTxn(op *OpTxn) {
	mgr.EnqueueRecevied(op)
}

func (mgr *TxnManager) onPreparCommit(txn *Transaction) {
	logrus.Infof("Prepare Committing %d", txn.Ctx.ID)
	if txn.PrepareCommitFn != nil {
		txn.Err = txn.PrepareCommitFn(txn)
	}
	if txn.Err != nil {
		return
	}
	txn.Err = txn.PreapreCommit()
}

func (mgr *TxnManager) onPreparRollback(txn *Transaction) {
	logrus.Infof("Prepare Rollbacking %d", txn.Ctx.ID)
	txn.Err = txn.PreapreRollback()
}

// TODO
func (mgr *TxnManager) onPreparing(items ...interface{}) {
	for _, item := range items {
		op := item.(*OpTxn)
		ts := mgr.TsAlloc.Alloc()
		mgr.Lock()
		op.Txn.Lock()
		if op.Op == OpCommit {
			op.Txn.Ctx.ToCommittingLocked(ts)
		} else if op.Op == OpRollback {
			op.Txn.Ctx.ToRollbackingLocked(ts)
		}
		op.Txn.Unlock()
		mgr.Unlock()
		if op.Op == OpCommit {
			mgr.onPreparCommit(op.Txn)
			if op.Txn.Err != nil {
				op.Op = OpRollback
				op.Txn.Lock()
				op.Txn.Ctx.ToRollbackingLocked(ts)
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
		op.Txn.Done()
		logrus.Infof("%d Committed", op.Txn.Ctx.ID)
	}
}
