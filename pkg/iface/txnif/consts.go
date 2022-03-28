package txnif

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
