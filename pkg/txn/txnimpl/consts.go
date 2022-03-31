package txnimpl

import "tae/pkg/iface/txnif"

const (
	TxnEntryCreateDatabase txnif.TxnEntryType = iota
	TxnEntryDropDatabase
	TxnEntryCretaeTable
	TxnEntryDropTable
)
