package demo

import (
	"github.com/RoaringBitmap/roaring"
)

type SnapshotBlockIt interface {
	Iterator
	GetBlock() ISnapshotBlock
}

// Snapshot bound block
type ISnapshotBlock interface {
	// Get block metadata
	GetBlockEntry() *BlockEntry
	// Get column data by attr name
	GetColumnDataByName(string) (Vector, error)
	// Get column data by attr idx
	GetColumnDataByID(int) (Vector, error)
	// Batch dedup
	// pks specify the primary keys to be inserted
	// rowmask specify the local deletes applied to the block
	BatchDedup(pks *Vector, rowmask *roaring.Bitmap) error
}

type ISnapshotRelation interface {
	// Get the table schema
	GetTableEntry() *TableEntry
	// Get the table row cnt
	Rows() int
	// Make a block iterator
	MakeBlockIt(*Expr) SnapshotBlockIt
}

type ISnapshotDatabase interface {
	// Get the db entry
	GetDBEntry() *DBEntry
	// Get a snapshot relation
	GetRelationByName(name string) (ISnapshotRelation, error)
}
