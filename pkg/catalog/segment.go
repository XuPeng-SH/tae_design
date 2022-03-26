package catalog

type SegmentEntry struct {
	*BaseEntry
	host    *TableEntry
	entries map[uint64]*BlockEntry
}

func (e *SegmentEntry) Compare(o NodePayload) int {
	oe := o.(*SegmentEntry).BaseEntry
	return e.DoCompre(oe)
}
