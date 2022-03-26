package catalog

type BlockEntry struct {
	*BaseEntry
	host  *SegmentEntry
	store *BlockStore
}
