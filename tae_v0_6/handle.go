package demo

import "github.com/RoaringBitmap/roaring"

type ILogReplaySM interface {
	// Atomic apply the specified batch commands
	// 1. Contiguous
	// SM: [0, 100], Cmds: [30,  50] -- OK
	// SM: [0, 100], Cmds: [90, 120] -- OK
	// SM: [0, 100], Cmds: [101,120] -- OK
	// SM: [0, 100], Cmds: [102,120] -- Error
	// 2. Idempotent
	// SM: [0, 100], Cmds: [30,  50] ==> SM: [0, 100]
	// SM: [0, 100], Cmds: [90, 120] ==> SM: [0, 120]
	// SM: [0, 100], Cmds: [101,120] ==> SM: [0, 120]
	// 3. Atomic
	ApplyCommands(*BatchCommands) (err error)
	// Get the state machine range desciption
	GetRangeDesc() RangeDesc
}

// acutally catalog.Catalog
type ICatalog interface {
	ILogReplaySM
	// Get table entry by db name and table
	GetTableEntryByName(ts Timestamp, dbName, tableName string) (*TableEntry, error)
	// Get table entry by table id
	GetTableEntryByID(id TableID) (*TableEntry, error)

	// Get db entry by db name
	GetDBEntryByName(ts Timestamp, dbName string) (*DBEntry, error)
	// Get db entry by db id
	GetDBEntryByID(id DBID) (*DBEntry, error)
}

// Block handle in a buffer object
type IBlock interface {
	ID() BlockID
	// Get data of the specified attr @ts
	// attr specifies the attr name
	// expr is a filter expression
	// ts is the timestamp of the read snapshot
	GetColumnDataByName(attr string, expr *Expr, ts Timestamp) (*Vector, error)
	// idx is the attr index in table schema
	GetColumnDataById(idx int, expr *Expr, ts Timestamp) (*Vector, error)

	// Batch dedup
	// pks specifies the primary keys to be inserted
	// rowmask specifies the local deletes applied to the block
	// ts is the timestamp of the read snapshot
	BatchDedup(pks *Vector, rowmask *roaring.Bitmap, ts Timestamp) error
}

type ISegment interface {
	ID() SegmentID
	// Get the block handle by block id
	GetBlockByID(blkId uint64) (IBlock, error)
	// Make a block iterator
	// expr is a filter expression
	// ts is the timestamp of the read snapshot
	MakeBlockIt(expr *Expr, ts Timestamp) Iterator
}

type ITable interface {
	ILogReplaySM
	// Get table entry
	GetEntry() (*TableEntry, error)
	// Get the segment handle by segment id
	GetSegmentByID(segId uint64) (ISegment, error)
	// Get the block handle by block id
	GetBlockByID(segId, blkId uint64) (IBlock, error)
	// Make a segment iterator
	// expr is a filter expression
	// ts is the timestamp of the read snapshot
	MakeSegmentIt(expr *Expr, ts Timestamp) Iterator
}
