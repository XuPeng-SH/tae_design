package catalog

import (
	"fmt"
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

func (entry *SegmentEntry) String() string {
	entry.RLock()
	defer entry.RUnlock()
	return fmt.Sprintf("SEGMENT%s", entry.BaseEntry.String())
}

func (entry *SegmentEntry) GetTable() *TableEntry {
	return entry.table
}

func (entry *SegmentEntry) Compare(o NodePayload) int {
	oe := o.(*SegmentEntry).BaseEntry
	return entry.DoCompre(oe)
}

// func (e *SegmentEntry) CreateSegmentEntry()
