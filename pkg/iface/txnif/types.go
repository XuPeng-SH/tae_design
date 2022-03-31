package txnif

import (
	"io"
	"sync"
	"tae/pkg/iface/handle"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type TxnReader interface {
	RLock()
	RUnlock()
	GetID() uint64
	GetStartTS() uint64
	GetCommitTS() uint64
	GetInfo() []byte
	IsTerminated(bool) bool
	IsVisible(o TxnReader) bool
	// Compare(o TxnReader) int
	GetTxnState(waitIfcommitting bool) int32
	GetError() error
	GetStore() TxnStore
	String() string
	Repr() string
}

type TxnHandle interface {
	// BatchDedup(uint64, *vector.Vector) error
	// RegisterTable(interface{}) error
	// GetTableByName(db, table string) (interface{}, error)
	// Append(uint64, *batch.Batch)
	CreateDatabase(name string) (handle.Database, error)
	DropDatabase(name string) (handle.Database, error)
	GetDatabase(name string) (handle.Database, error)
	UseDatabase(name string) error
}

type TxnChanger interface {
	sync.Locker
	RLock()
	RUnlock()
	ToCommittedLocked() error
	ToCommittingLocked(ts uint64) error
	ToRollbackedLocked() error
	ToRollbackingLocked(ts uint64) error
	Commit() error
	Rollback() error
	PrepareCommit() error
	PrepareRollback() error
	DoCommit() error
	SetError(error)
	SetPrepareCommitFn(func(interface{}) error)
}

type TxnWriter interface {
}

type TxnAsyncer interface {
	WaitDone() error
}

type AsyncTxn interface {
	TxnHandle
	TxnAsyncer
	TxnReader
	TxnWriter
	TxnChanger
}

type SyncTxn interface {
	TxnReader
	TxnWriter
	TxnChanger
}

type BlockUpdates interface {
	GetID() *common.ID
	DeleteLocked(start, end uint32) error
	UpdateLocked(row uint32, colIdx uint16, v interface{}) error
	GetColumnUpdatesLocked(colIdx uint16) ColumnUpdates
	MergeColumnLocked(o BlockUpdates, colIdx uint16) error
	ReadFrom(r io.Reader) error
	WriteTo(w io.Writer) error
}
type ColumnUpdates interface {
	ReadFrom(r io.Reader) error
	WriteTo(w io.Writer) error
	Update(row uint32, v interface{}) error
	UpdateLocked(row uint32, v interface{}) error
	MergeLocked(o ColumnUpdates) error
	ApplyToColumn(vec *vector.Vector, deletes *roaring.Bitmap) *vector.Vector
}

type TxnStore interface {
	io.Closer
	BindTxn(AsyncTxn)

	Append(id uint64, data *batch.Batch) error
	RangeDeleteLocalRows(id uint64, start, end uint32) error
	UpdateLocalValue(id uint64, row uint32, col uint16, v interface{}) error
	AddUpdateNode(id uint64, node BlockUpdates) error

	CreateRelation(def interface{}) (handle.Relation, error)
	DropRelationByName(name string) (handle.Relation, error)
	GetRelationByName(name string) (handle.Relation, error)

	CreateDatabase(name string) (handle.Database, error)
	GetDatabase(name string) (handle.Database, error)
	DropDatabase(name string) (handle.Database, error)
	UseDatabase(name string) error

	AddTxnEntry(TxnEntryType, TxnEntry)

	PrepareCommit() error
	// PrepareRollback() error
	Commit() error
	// Rollback() error
}

type TxnEntryType int16

type TxnEntry interface {
	sync.Locker
	RLock()
	RUnlock()
	PrepareCommit() error
	// PrepareRollback() error
	Commit() error
	// Rollback() error
}
