package catalog

type SegmentEntry struct {
	*BaseEntry
	host    *TableEntry
	entries map[uint64]*BlockEntry
}
