package catalog

import "tae/pkg/iface"

type TableEntry struct {
	*BaseEntry
	db      *DBEntry
	entries map[uint64]*DLNode
	link    *Link
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

func (entry *TableEntry) prepareAddEntry(txn iface.AsyncTxn) {

}
