package demo

import (
	"github.com/RoaringBitmap/roaring"
)

type Batch struct{}   // container.Batch
type Index struct{}   // index.Index
type Zonemap struct{} // index.Zonemap

type DataHeader struct {
	// Created transaction timestamp
	// Should be nil for non-appendable block
	Min *Vector

	// Deleted transaction timestamp
	// Should be nil if no deletes
	Max *Vector

	// Dead rows. Rows from a aborted transaction
	// Should be nil if no dead rows
	Dead *roaring.Bitmap
}

type BlockData struct {
	// Data header for visible check
	Header *DataHeader
	// All columns data
	Columns *Batch
	// Index
	Index *Index
}

// Colocated in BlockMetaObj
type BlockHeader struct {
	MinKey string
	MaxKey string
	Dead   *roaring.Bitmap
}

// Cache item
type BlockMetaObj struct {
	// All columns
	AttrKeys []string
	BFKeys   map[int]string
	ZMs      []*Zonemap

	// Block header for data visibility check
	Header *BlockHeader
}

// Cache item
type BlockDataObj struct {
	// Block metadata
	Entry *BlockEntry

	// Block mvcc metadata cache key
	MetaKeys map[Timestamp]string

	// Block index
	Index *Index
}

type MutableNode struct {
	// Blocks of data synced from DN
	// Key is the block id
	Blocks map[uint64]*BlockData

	// Deletes of data synced from DN
	// Key is the block id
	Deletes map[uint64]*Vector
}

// Cache item
type TableDataObj struct {
	// Table metadata tree
	Entry *TableEntry

	// Block metadata cache keys
	BlockKeys map[uint64]string

	// Block data synced from DN
	Mutation *MutableNode
}
