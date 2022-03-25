package txn

import "errors"

var (
	ErrTxnAlreadyCommitted = errors.New("tae: txn already committed")
	ErrTxnNotCommitting    = errors.New("tae: txn not commiting")
	ErrTxnNotRollbacking   = errors.New("tae: txn not rollbacking")
	ErrTxnNotActive        = errors.New("tae: txn not active")
)
