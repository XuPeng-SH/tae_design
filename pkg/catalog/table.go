package catalog

import "tae/pkg/iface"

type TableEntry struct {
	*BaseEntry
	db      *DBEntry
	entries map[uint64]*SegmentEntry
	link    *Link
}

func (entry *TableEntry) addEntryLocked(segment *SegmentEntry) {
	entry.entries[segment.GetID()] = segment
	entry.link.Insert(segment)
}

func (entry *TableEntry) AddEntry(txn iface.AsyncTxn) {

}
