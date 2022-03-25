package txn

import (
	"fmt"
	"sync/atomic"
)

const (
	UncommitTS = ^uint64(0)
)

type TxnCtx struct {
	ID                uint64
	StartTS, CommitTS uint64
	Info              []byte
	Rollbacked        int32
}

func (ctx *TxnCtx) IsTerminated() bool {
	return UncommitTS != atomic.LoadUint64(&ctx.StartTS)
}

func (ctx *TxnCtx) HasRollbacked() bool {
	return atomic.LoadInt32(&ctx.Rollbacked) != 0
}

func (ctx *TxnCtx) Commit(ts uint64) error {
	if ts <= ctx.StartTS {
		panic(fmt.Sprintf("start ts %d should be less than commit ts %d", ctx.StartTS, ts))
	}
	if !atomic.CompareAndSwapUint64(&ctx.CommitTS, UncommitTS, ts) {
		return ErrTxnAlreadyCommitted
	}
	return nil
}

func (ctx *TxnCtx) Rollback(ts uint64) error {
	if err := ctx.Commit(ts); err != nil {
		return err
	}
	atomic.StoreInt32(&ctx.Rollbacked, int32(1))
	return nil
}
