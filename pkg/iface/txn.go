package iface

import "sync"

type TxnReader interface {
	GetID() uint64
	GetStartTS() uint64
	GetInfo() []byte
	IsTerminated() bool
	Compare(o TxnReader) int
	GetTxnState(waitIfcommitting bool) int32
	GetError() error
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
