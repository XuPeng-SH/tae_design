package catalog

import (
	"fmt"
	"sync"
	"tae/pkg/iface/txnif"
)

type TableEntry struct {
	*BaseEntry2
	db      *DBEntry
	schema  *Schema
	entries map[uint64]*DLNode
	link    *Link
}

func NewTableEntry(db *DBEntry, schema *Schema, txnCtx txnif.AsyncTxn) *TableEntry {
	id := db.catalog.NextTable()
	e := &TableEntry{
		BaseEntry2: &BaseEntry2{
			CommitInfo2: CommitInfo2{
				Txn:    txnCtx,
				CurrOp: OpCreate,
			},
			RWMutex: new(sync.RWMutex),
			ID:      id,
		},
		db:     db,
		schema: schema,
	}
	return e
}

// func (entry *TableEntry) ToLogEntry() LogEntry {

// }

func (entry *TableEntry) addEntryLocked(segment *SegmentEntry) {
	n := entry.link.Insert(segment)
	entry.entries[segment.GetID()] = n
}

func (entry *TableEntry) deleteEntryLocked(segment *SegmentEntry) error {
	if n, ok := entry.entries[segment.GetID()]; !ok {
		return ErrNotFound
	} else {
		entry.link.Delete(n)
	}
	return nil
}

func (entry *TableEntry) Compare(o NodePayload) int {
	oe := o.(*TableEntry).BaseEntry2
	return entry.DoCompre(oe)
}

func (entry *TableEntry) String() string {
	return fmt.Sprintf("TABLE%s[name=%s]", entry.BaseEntry2.String(), entry.schema.Name)
	// s := fmt.Sprintf("TABLE<%d>[\"%s\"]: [%d-%d],[%d-%d]", entry.ID, entry.schema.Name, entry.CreateStartTS, entry.CreateCommitTS, entry.DropStartTS, entry.DropCommitTS)
	// s := fmt.Sprintf("TABLE<%d>[\"%s\"]: [%d-%d]", entry.ID, entry.schema.Name, entry.CreateAt, entry.DeleteAt)
	// if entry.Txn != nil {
	// 	s = fmt.Sprintf("%s, [%d-%d]", s, entry.Txn.GetStartTS(), entry.Txn.GetCommitTS())
	// }
	// return s
}
