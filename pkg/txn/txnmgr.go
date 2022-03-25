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
	pqueue := sm.NewSafeQueue(10000, 200, mgr.onPrepareCommit)
	cqueue := sm.NewSafeQueue(10000, 200, mgr.onCommit)
	mgr.StateMachine = sm.NewStateMachine(new(sync.WaitGroup), mgr, pqueue, cqueue)
	return mgr
}

func (mgr *TxnManager) InitStartCtx(prevTxnId uint64, prevTs uint64) error {
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

func (mgr *TxnManager) OnCommitTxn(txn *Transaction) {
	mgr.EnqueueRecevied(txn)
}

// TODO
func (mgr *TxnManager) onPrepareCommit(items ...interface{}) {
	for _, item := range items {
		txn := item.(*Transaction)
		ts := mgr.TsAlloc.Alloc()
		txn.Ctx.ToCommittingLocked(ts)
		logrus.Infof("Prepare Committing %d", txn.Ctx.ID)
		if txn.PrepareCommitFn != nil {
			txn.Err = txn.PrepareCommitFn(txn)
		}
		mgr.EnqueueCheckpoint(txn)
	}
}

// TODO
func (mgr *TxnManager) onCommit(items ...interface{}) {
	for _, item := range items {
		txn := item.(*Transaction)
		txn.Done()
		logrus.Infof("%d Committed", txn.Ctx.ID)
	}
}
