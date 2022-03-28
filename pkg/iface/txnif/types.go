package txnif

import (
	"io"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type TxnReader interface {
	GetID() uint64
	GetStartTS() uint64
	GetCommitTS() uint64
	GetInfo() []byte
	IsTerminated(bool) bool
	Compare(o TxnReader) int
	GetTxnState(waitIfcommitting bool) int32
	GetError() error
}

type TxnHandle interface {
	BatchDedup(uint64, *vector.Vector) error
	RegisterTable(interface{}) error
	GetTableByName(db, table string) (interface{}, error)
	Append(uint64, *batch.Batch)
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
	PreapreCommit() error
	PreapreRollback() error
	SetError(error)
	SetPrepareCommitFn(func(interface{}) error)
}

type TxnWriter interface {
}

type TxnAsyncer interface {
	WaitDone() error
}

type AsyncTxn interface {
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
	Append(id uint64, data *batch.Batch) error
	RangeDeleteLocalRows(id uint64, start, end uint32) error
	UpdateLocalValue(id uint64, row uint32, col uint16, v interface{}) error
	AddUpdateNode(id uint64, node BlockUpdates) error
}
