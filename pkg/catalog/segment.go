package catalog

import (
	"sync"
	"tae/pkg/iface/txnif"
)

type SegmentEntry struct {
	*BaseEntry
	table   *TableEntry
	entries map[uint64]*BlockEntry
	link    *Link
}

func NewSegmentEntry(table *TableEntry, txn txnif.AsyncTxn) *SegmentEntry {
	id := table.GetDB().catalog.NextSegment()
	e := &SegmentEntry{
		BaseEntry: &BaseEntry{
			CommitInfo: CommitInfo{
				Txn:    txn,
				CurrOp: OpCreate,
			},
			RWMutex: new(sync.RWMutex),
			ID:      id,
		},
		table: table,
	}
	return e
}

func (e *SegmentEntry) GetTable() *TableEntry {
	return e.table
}

func (e *SegmentEntry) Compare(o NodePayload) int {
	oe := o.(*SegmentEntry).BaseEntry
	return e.DoCompre(oe)
}

// func (e *SegmentEntry) CreateSegmentEntry()
