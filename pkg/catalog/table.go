package catalog

import (
	"fmt"
	"sync"
	"tae/pkg/iface"
)

type TableEntry struct {
	*BaseEntry
	db      *DBEntry
	schema  *Schema
	entries map[uint64]*DLNode
	link    *Link
}

func NewTableEntry(db *DBEntry, schema *Schema, txnCtx iface.TxnReader) *TableEntry {
	id := db.catalog.NextTable()
	e := &TableEntry{
		BaseEntry: &BaseEntry{
			CommitInfo: CommitInfo{
				CreateStartTS:  txnCtx.GetStartTS(),
				CreateCommitTS: UncommitTS,
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
	oe := o.(*TableEntry).BaseEntry
	return entry.DoCompre(oe)
}

func (entry *TableEntry) String() string {
	s := fmt.Sprintf("TABLE<%d>[\"%s\"]: [%d-%d],[%d-%d]", entry.ID, entry.schema.Name, entry.CreateStartTS, entry.CreateCommitTS, entry.DropStartTS, entry.DropCommitTS)
	return s
}
