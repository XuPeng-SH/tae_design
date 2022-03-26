package catalog

import "sync"

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
}

func IsCommitted(ts uint64) bool {
	return ts != UncommitTS
}

type CommitInfo struct {
	StartTS, CommitTS uint64
}

type BaseEntry struct {
	*CommitInfo
	*sync.RWMutex
	ID       uint64
	CreateAt uint64
	DeleteAt uint64
}

func (e *BaseEntry) GetID() uint64 { return e.ID }

func (e *BaseEntry) Compare(o NodePayload) int {
	oe := o.(*BaseEntry)
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
	return CompareUint64(e.StartTS, oe.StartTS)
}
