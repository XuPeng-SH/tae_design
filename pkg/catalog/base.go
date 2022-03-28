package catalog

import (
	"sync"
	"tae/pkg/iface"
	"tae/pkg/txn"
)

const (
	UncommitTS = ^uint64(0)
)

func CompareUint64(left, right uint64) int {
	if left > right {
		return 1
	} else if left < right {
		return -1
	}
	return 0
}

type CommandInfo struct {
	Op OpT
}

func IsCommitted(ts uint64) bool {
	return ts != UncommitTS
}

type CommitInfo struct {
	CommandInfo
	CreateStartTS, CreateCommitTS uint64
	DropStartTS, DropCommitTS     uint64
}

type BaseEntry struct {
	*sync.RWMutex
	CommitInfo
	ID       uint64
	CreateAt uint64
	DeleteAt uint64
}

func (e *BaseEntry) GetID() uint64 { return e.ID }

func (e *BaseEntry) DoCompre(oe *BaseEntry) int {
	ecommitted := IsCommitted(e.CreateAt)
	oecommitted := IsCommitted(oe.CreateAt)
	if ecommitted && !oecommitted {
		return -1
	}
	if !ecommitted && oecommitted {
		return 1
	}
	if ecommitted && oecommitted {
		return CompareUint64(e.CreateAt, oe.CreateAt)
	}
	return CompareUint64(e.CreateStartTS, oe.CreateStartTS)
}

func (e *BaseEntry) IsSameTxn(ts uint64) bool {
	return e.CreateStartTS == ts || (e.DropStartTS != 0 && e.DropStartTS == ts)
}

func (e *BaseEntry) IsSameTxnEntry(o *BaseEntry) bool {
	// logrus.Infof("kkkkk-%d:%d----%d:%d", e.CreateStartTS, e.DropStartTS, o.CreateStartTS, o.DropStartTS)
	return e.CreateStartTS == o.CreateStartTS || (e.DropStartTS != 0 && e.DropStartTS == o.DropStartTS)
}

func (e *BaseEntry) IsDroppedCommitted() bool {
	return e.DeleteAt != 0
}

func (e *BaseEntry) IsDroppedUncommitted() bool {
	return e.DeleteAt == 0 && e.DropStartTS != 0
}

func (e *BaseEntry) HasStarted() bool {
	return e.CreateAt != 0
}

func (e *BaseEntry) HasDropped() bool {
	return e.DeleteAt != 0
}

func (e *BaseEntry) CommitStart(ts uint64) error {
	if e.HasStarted() {
		panic("unexpected")
	}
	e.CommitInfo.CreateCommitTS = ts
	e.CreateAt = ts
	return nil
}

func (e *BaseEntry) CommitDrop(ts uint64) error {
	if e.HasDropped() {
		panic("unexpected")
	}
	e.CommitInfo.DropCommitTS = ts
	e.DeleteAt = ts
	return nil
}

func (e *BaseEntry) DropEntryLocked(txnCtx iface.TxnReader) error {
	startTS := txnCtx.GetStartTS()
	if e.HasDropped() {
		return ErrValidation
	}
	if e.DropStartTS != 0 {
		return txn.TxnWWConflictErr
	}
	if e.HasStarted() {
		if startTS <= e.CreateAt {
			return ErrValidation
		}
		e.DropStartTS = startTS
		e.DropCommitTS = UncommitTS
	} else {
		if !e.IsSameTxn(startTS) {
			return ErrValidation
		}
		// In a same txn, if create then drop:
		// CreateStartTS == DropStartTS
		// CreateCommitTS == DropCommitTS
		// The name node should be deleted from nameNodes during committing
		e.DropStartTS = startTS
		e.DropCommitTS = UncommitTS
	}
	return nil
}
