package txnif

import "errors"

var (
	TxnWWConflictErr = errors.New("tae: w-w conflict error")
)
