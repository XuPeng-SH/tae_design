package txnif

import "errors"

var (
	TxnRWConflictErr = errors.New("tae: r-w conflict error")
	TxnWWConflictErr = errors.New("tae: w-w conflict error")
)
