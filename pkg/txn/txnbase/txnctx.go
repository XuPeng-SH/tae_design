package txnbase

import (
	"fmt"
	"sync"
	"tae/pkg/iface/txnif"
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
		CommitTS: txnif.UncommitTS,
		Info:     info,
	}
}

func (ctx *TxnCtx) Repr() string {
	repr := fmt.Sprintf("Txn[%d][%d->%d][%s]", ctx.ID, ctx.StartTS, ctx.CommitTS, txnif.TxnStrState(ctx.State))
	return repr
}

func (ctx *TxnCtx) String() string     { return ctx.Repr() }
func (ctx *TxnCtx) GetID() uint64      { return ctx.ID }
func (ctx *TxnCtx) GetInfo() []byte    { return ctx.Info }
func (ctx *TxnCtx) GetStartTS() uint64 { return ctx.StartTS }
func (ctx *TxnCtx) GetCommitTS() uint64 {
	ctx.RLock()
	defer ctx.RUnlock()
	return ctx.CommitTS
}

func (ctx *TxnCtx) Compare(o txnif.TxnReader) int {
	return 0
}

func (ctx *TxnCtx) IsActiveLocked() bool {
	return ctx.CommitTS == txnif.UncommitTS
}

func (ctx *TxnCtx) ToCommittingLocked(ts uint64) error {
	if ts <= ctx.StartTS {
		panic(fmt.Sprintf("start ts %d should be less than commit ts %d", ctx.StartTS, ts))
	}
	if ctx.CommitTS != txnif.UncommitTS {
		return ErrTxnNotActive
	}
	ctx.CommitTS = ts
	ctx.State = txnif.TxnStateCommitting
	return nil
}

func (ctx *TxnCtx) ToCommittedLocked() error {
	if ctx.State != txnif.TxnStateCommitting {
		return ErrTxnNotCommitting
	}
	ctx.State = txnif.TxnStateCommitted
	return nil
}

func (ctx *TxnCtx) ToRollbackingLocked(ts uint64) error {
	if ts <= ctx.StartTS {
		panic(fmt.Sprintf("start ts %d should be less than commit ts %d", ctx.StartTS, ts))
	}
	if (ctx.State != txnif.TxnStateActive) && (ctx.State != txnif.TxnStateCommitting) {
		return ErrTxnCannotRollback
	}
	ctx.CommitTS = ts
	ctx.State = txnif.TxnStateRollbacking
	return nil
}

func (ctx *TxnCtx) ToRollbackedLocked() error {
	if ctx.State != txnif.TxnStateRollbacking {
		return ErrTxnNotRollbacking
	}
	ctx.State = txnif.TxnStateRollbacked
	return nil
}
