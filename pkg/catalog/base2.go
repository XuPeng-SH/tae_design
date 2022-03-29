package catalog

import (
	"fmt"
	"sync"
	"tae/pkg/iface/txnif"
)

type Waitable interface {
	Wait() error
}

type waitable struct {
	fn func() error
}

func (w *waitable) Wait() error {
	return w.fn()
}

type CommitInfo2 struct {
	// Ops    []OpT
	CurrOp OpT
	Txn    txnif.TxnReader
}

type BaseEntry2 struct {
	*sync.RWMutex
	CommitInfo2
	ID                 uint64
	CreateAt, DeleteAt uint64
}

func (be *BaseEntry2) IsTerminated(waitIfcommitting bool) bool {
	return be.Txn.IsTerminated(waitIfcommitting)
}

func (be *BaseEntry2) IsCommitted() bool {
	if be.Txn == nil {
		return true
	}
	state := be.Txn.GetTxnState(true)
	return state == txnif.TxnStateCommitted || state == txnif.TxnStateRollbacked
}

func (be *BaseEntry2) GetID() uint64 { return be.ID }

func (be *BaseEntry2) DoCompre(oe *BaseEntry2) int {
	be.RLock()
	defer be.RUnlock()
	oe.RLock()
	defer oe.RUnlock()
	r := 0
	if be.CreateAt != 0 && oe.CreateAt != 0 {
		r = CompareUint64(be.CreateAt, oe.CreateAt)
	} else if be.CreateAt != 0 {
		r = -1
	} else if oe.CreateAt != 0 {
		r = 1
	} else {
		r = CompareUint64(be.Txn.GetStartTS(), oe.Txn.GetStartTS())
	}
	return r
}

func (be *BaseEntry2) PrepareCommit() error {
	be.Lock()
	defer be.Unlock()
	if be.CreateAt == 0 {
		be.CreateAt = be.Txn.GetCommitTS()
	}
	if be.CurrOp == OpSoftDelete {
		be.DeleteAt = be.Txn.GetCommitTS()
	}
	return nil
}

func (be *BaseEntry2) Commit() error {
	be.Lock()
	defer be.Unlock()
	if be.Txn == nil {
		panic("logic error")
	}
	be.Txn = nil
	return nil
}

func (be *BaseEntry2) HasDropped() bool {
	return be.DeleteAt != 0
}

func (be *BaseEntry2) CreateBefore(ts uint64) bool {
	if be.CreateAt != 0 {
		return be.CreateAt < ts
	}
	return false
}

func (be *BaseEntry2) CreateAfter(ts uint64) bool {
	if be.CreateAt != 0 {
		return be.CreateAt > ts
	}
	return false
}

func (be *BaseEntry2) DeleteBefore(ts uint64) bool {
	if be.DeleteAt != 0 {
		return be.DeleteAt < ts
	}
	return false
}

func (be *BaseEntry2) DeleteAfter(ts uint64) bool {
	if be.DeleteAt != 0 {
		return be.DeleteAt > ts
	}
	return false
}

func (be *BaseEntry2) HasCreated() bool {
	return be.CreateAt != 0
}

func (be *BaseEntry2) DropEntryLocked(txnCtx txnif.TxnReader) error {
	if be.Txn == nil {
		if be.HasDropped() {
			return ErrNotFound
		}
		if be.CreateAt > txnCtx.GetStartTS() {
			panic("unexpected")
		}
		be.Txn = txnCtx
		be.CurrOp = OpSoftDelete
		return nil
	}
	if be.Txn.GetID() == txnCtx.GetID() {
		if be.CurrOp == OpSoftDelete {
			return ErrNotFound
		}
		be.CurrOp = OpSoftDelete
		return nil
	}
	return txnif.TxnWWConflictErr
}

func (be *BaseEntry2) SameTxn(o *BaseEntry2) bool {
	if be.Txn != nil && o.Txn != nil {
		return be.Txn.GetID() == o.Txn.GetID()
	}
	return false
}

func (be *BaseEntry2) IsDroppedUncommitted() bool {
	if be.Txn != nil {
		return be.CurrOp == OpSoftDelete
	}
	return false
}

func (be *BaseEntry2) HasActiveTxn() bool {
	return be.Txn != nil
}

func (be *BaseEntry2) GetTxnID() uint64 {
	if be.Txn != nil {
		return be.Txn.GetID()
	}
	return 0
}

func (be *BaseEntry2) IsSameTxn(ctx txnif.TxnReader) bool {
	if be.Txn != nil {
		return be.Txn.GetID() == ctx.GetID()
	}
	return false
}

func (be *BaseEntry2) IsCommitting() bool {
	if be.Txn != nil && be.Txn.GetCommitTS() != txnif.UncommitTS {
		return true
	}
	return false
}

func (be *BaseEntry2) CreateAndDropInSameTxn() bool {
	if be.CreateAt != 0 && (be.CreateAt == be.DeleteAt) {
		return true
	}
	return false
}

func (be *BaseEntry2) String() string {
	s := fmt.Sprintf("[ID=%d][%d,%d]", be.ID, be.CreateAt, be.DeleteAt)
	if be.Txn != nil {
		s = fmt.Sprintf("%s%s", s, be.Txn.Repr())
	}
	return s
}
