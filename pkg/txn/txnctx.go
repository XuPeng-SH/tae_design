package txn

import (
	"fmt"
	"sync"
	"tae/pkg/iface"
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

type TxnCtx struct {
	*sync.RWMutex
	ID                uint64
	StartTS, CommitTS uint64
	Info              []byte
	State             int32
}

func NewTxnCtx(rwlocker *sync.RWMutex, id, start uint64, info []byte) *TxnCtx {
	if rwlocker == nil {
		rwlocker = new(sync.RWMutex)
	}
	return &TxnCtx{
		ID:       id,
		RWMutex:  rwlocker,
		StartTS:  start,
		CommitTS: UncommitTS,
		Info:     info,
	}
}

func (ctx *TxnCtx) GetID() uint64       { return ctx.ID }
func (ctx *TxnCtx) GetInfo() []byte     { return ctx.Info }
func (ctx *TxnCtx) GetStartTS() uint64  { return ctx.StartTS }
func (ctx *TxnCtx) GetCommitTS() uint64 { return ctx.CommitTS }

func (ctx *TxnCtx) Compare(o iface.TxnReader) int {
	return 0
}

func (ctx *TxnCtx) IsActiveLocked() bool {
	return ctx.CommitTS == UncommitTS
}

func (ctx *TxnCtx) ToCommittingLocked(ts uint64) error {
	if ts <= ctx.StartTS {
		panic(fmt.Sprintf("start ts %d should be less than commit ts %d", ctx.StartTS, ts))
	}
	if ctx.CommitTS != UncommitTS {
		return ErrTxnNotActive
	}
	ctx.CommitTS = ts
	ctx.State = TxnStateCommitting
	return nil
}

func (ctx *TxnCtx) ToCommittedLocked() error {
	if ctx.State != TxnStateCommitting {
		return ErrTxnNotCommitting
	}
	ctx.State = TxnStateCommitted
	return nil
}

func (ctx *TxnCtx) ToRollbackingLocked(ts uint64) error {
	if ts <= ctx.StartTS {
		panic(fmt.Sprintf("start ts %d should be less than commit ts %d", ctx.StartTS, ts))
	}
	if (ctx.State != TxnStateActive) && (ctx.State != TxnStateCommitting) {
		return ErrTxnCannotRollback
	}
	ctx.CommitTS = ts
	ctx.State = TxnStateRollbacking
	return nil
}

func (ctx *TxnCtx) ToRollbackedLocked() error {
	if ctx.State != TxnStateRollbacking {
		return ErrTxnNotRollbacking
	}
	ctx.State = TxnStateRollbacked
	return nil
}
