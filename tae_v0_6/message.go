package demo

type DBID = uint64

type TableID struct {
	ID   uint64
	DBID uint64
}

type SegmentID struct {
	DBID    uint64
	TableID uint64
	ID      uint64
}

type BlockID struct {
	DBID      uint64
	TableID   uint64
	SegmentID uint64
	BlockID   uint64
}

// [From, To]
type RangeDesc struct {
	From Timestamp
	To   Timestamp
}

type BatchCommands struct {
	// Commands description
	Desc RangeDesc
	// A slice of []byte, each is a serialized ITxnCmd
	Serialized   [][]byte
	Deserialized []ITxnCmd
}

type SnapshotSyncReq struct {
	// Most suitable visible checkpoint timestamp
	CheckpointTS Timestamp
	// [FromTS, SnapshotTS]
	SnapshotTS Timestamp
	FromTS     Timestamp

	// Table ids to read
	Tables []TableID
	// If true, read all tables
	// Else, read the specified tables
	All bool
}

type SnapshotSyncResp struct {
	// Snapshot timestamp
	SnapshotTS Timestamp

	// Checkpoint timestamp in request
	CheckpointTS Timestamp

	// New checkpoints found in DN
	NewCheckpoints []Timestamp

	// Catalog commands
	CatalogCmds *BatchCommands

	// Table Commands
	TableCmds map[TableID]*BatchCommands
}

type SnapshotCommitReq struct {
	SnapshotTS Timestamp
	Command    ITxnCmd
}
