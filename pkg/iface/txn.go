package iface

import (
	"errors"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
)

var (
	TxnWWConflictErr = errors.New("tae: w-w conflict error")
)

const (
	UncommitTS = ^uint64(0)
)

const (
	TxnStateActive int32 = iota
	TxnStateCommitting
	TxnStateRollbacking
	TxnStateCommitted
	TxnStateRollbacked
)

type TxnReader interface {
	GetID() uint64
	GetStartTS() uint64
	GetCommitTS() uint64
	GetInfo() []byte
	IsTerminated(bool) bool
	Compare(o TxnReader) int
	GetTxnState(waitIfcommitting bool) int32
	GetError() error
}

type TxnStore interface {
	BatchDedup(uint64, *vector.Vector) error
	RegisterTable(interface{}) error
	GetTableByName(db, table string) (interface{}, error)
	Append(uint64, *batch.Batch)
}

type TxnChanger interface {
	sync.Locker
	RLock()
	RUnlock()
	ToCommittedLocked() error
	ToCommittingLocked(ts uint64) error
	ToRollbackedLocked() error
	ToRollbackingLocked(ts uint64) error
	Commit() error
	Rollback() error
	PreapreCommit() error
	PreapreRollback() error
	SetError(error)
	SetPrepareCommitFn(func(interface{}) error)
}

type TxnWriter interface {
}

type TxnAsyncer interface {
	WaitDone() error
}

type AsyncTxn interface {
	TxnAsyncer
	TxnReader
	TxnWriter
	TxnChanger
}

type SyncTxn interface {
	TxnReader
	TxnWriter
	TxnChanger
}