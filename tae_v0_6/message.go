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

type ScopeDesc struct {
	// True indicates all tables
	// False indicates the specified tables
	All bool

	// Indicates the specified tables when All is false
	Tables []TableID
}

type BatchCommands struct {
	// Range description
	Desc RangeDesc
	// Scope description
	Scope ScopeDesc

	Commands []ICommand
}

type SyncLogTailReq struct {
	// Most suitable visible checkpoint timestamp
	CheckpointTS Timestamp

	// [FromTS, ToTS]
	Range RangeDesc

	// Table ids to read
	Tables []TableID

	// If true, read all tables
	// Else, read the specified tables
	All bool
}

type SyncLogTailResp struct {
	// Actual checkpoint timestamp
	CheckpointTS Timestamp

	// New checkpoints found in DN
	NewCheckpoints []Timestamp

	// Tail commands
	Commands *BatchCommands
}

type PreCommitWriteMsg struct {
	SnapshotTS Timestamp
	// --[DB1]
	//      |--<CreateDB>           [1-0]
	//      |--[Table1-1]
	//      |      |--<CreateTable> [1-1]
	//      |      |--<AppendData>  [1-2]
	//      |      |--<DeleteData>  [1-3]
	//      |      |--<AddBlock>    [1-4]
	//      |      |--<DeleteBlock> [1-5]
	//      |      |--<DropTable>   [1-6]
	//      |
	//      |--[Table1-2]
	//      |      |--<CreateTable> [1-7]
	//      |      |--<AppendData>  [1-8]
	//      |      |--<DeleteData>  [1-9]
	//      |      |--<AddBlock>    [1-10]
	//      |      |--<DeleteBlock> [1-11]
	//      |      |--<DropTable>   [1-12]
	//      |
	//      |--<DropDB>             [1-13]
	//
	// --[DB2]
	//      |--<DropDB>             [1-14]
	//
	// --[DB3]
	//      |--<CreateDB>           [1-15]
	//
	// The above message contains total 16 subcommands, which must remain in order
	Command ITxnCmd
}
