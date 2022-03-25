package txn

import "errors"

var (
	ErrTxnAlreadyCommitted = errors.New("tae: already committed")
)
