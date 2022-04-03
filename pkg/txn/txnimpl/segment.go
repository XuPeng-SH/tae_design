package txnimpl

import (
	"tae/pkg/catalog"
	"tae/pkg/iface/txnif"
	"tae/pkg/txn/txnbase"
)

type txnSegment struct {
	*txnbase.TxnSegment
	entry *catalog.SegmentEntry
	store txnif.TxnStore
}

func newSegment(txn txnif.AsyncTxn, meta *catalog.SegmentEntry) *txnSegment {
	seg := &txnSegment{
		TxnSegment: &txnbase.TxnSegment{
			Txn: txn,
		},
		entry: meta,
	}
	return seg
}

func (seg *txnSegment) GetMeta() interface{} { return seg.entry }
func (seg *txnSegment) String() string       { return seg.entry.String() }
func (seg *txnSegment) GetID() uint64        { return seg.entry.GetID() }
