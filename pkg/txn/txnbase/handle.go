package txnbase

import (
	"tae/pkg/iface/handle"
	"tae/pkg/iface/txnif"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type TxnDatabase struct {
	Txn txnif.AsyncTxn
}

type TxnRelation struct {
	Txn txnif.AsyncTxn
	DB  handle.Database
}

type TxnSegment struct {
	Txn txnif.AsyncTxn
	Rel handle.Relation
}

type TxnBlock struct {
	Txn txnif.AsyncTxn
	Seg handle.Segment
}

func (db *TxnDatabase) GetID() uint64                                                   { return 0 }
func (db *TxnDatabase) GetName() string                                                 { return "" }
func (db *TxnDatabase) String() string                                                  { return "" }
func (db *TxnDatabase) Close() error                                                    { return nil }
func (db *TxnDatabase) CreateRelation(def interface{}) (rel handle.Relation, err error) { return }
func (db *TxnDatabase) DropRelationByName(name string) (rel handle.Relation, err error) { return }
func (db *TxnDatabase) GetRelationByName(name string) (rel handle.Relation, err error)  { return }
func (db *TxnDatabase) RelationCnt() int64                                              { return 0 }
func (db *TxnDatabase) Relations() (rels []handle.Relation)                             { return }
func (db *TxnDatabase) MakeRelationIt() (it handle.RelationIt)                          { return }
func (db *TxnDatabase) GetMeta() interface{}                                            { return nil }

func (rel *TxnRelation) String() string                                 { return "" }
func (rel *TxnRelation) Close() error                                   { return nil }
func (rel *TxnRelation) ID() uint64                                     { return 0 }
func (rel *TxnRelation) Rows() int64                                    { return 0 }
func (rel *TxnRelation) Size(attr string) int64                         { return 0 }
func (rel *TxnRelation) GetCardinality(attr string) int64               { return 0 }
func (rel *TxnRelation) Schema() interface{}                            { return nil }
func (rel *TxnRelation) MakeSegmentIt() handle.SegmentIt                { return nil }
func (rel *TxnRelation) MakeReader() handle.Reader                      { return nil }
func (rel *TxnRelation) BatchDedup(col *vector.Vector) error            { return nil }
func (rel *TxnRelation) Append(data *batch.Batch) error                 { return nil }
func (rel *TxnRelation) GetMeta() interface{}                           { return nil }
func (rel *TxnRelation) CreateSegment() (seg handle.Segment, err error) { return }

func (seg *TxnSegment) GetMeta() interface{}               { return nil }
func (seg *TxnSegment) String() string                     { return "" }
func (seg *TxnSegment) Close() error                       { return nil }
func (seg *TxnSegment) GetID() uint64                      { return 0 }
func (seg *TxnSegment) MakeBlockIt() (it handle.BlockIt)   { return }
func (seg *TxnSegment) MakeReader() (reader handle.Reader) { return }

func (seg *TxnSegment) GetByFilter(handle.Filter, bool) (bats map[uint64]*batch.Batch, err error) {
	return
}

func (seg *TxnSegment) Append(*batch.Batch, uint32) (n uint32, err error)      { return }
func (seg *TxnSegment) Update(uint64, uint32, uint16, interface{}) (err error) { return }
func (seg *TxnSegment) RangeDelete(uint64, uint32, uint32) (err error)         { return }

func (seg *TxnSegment) PushDeleteOp(handle.Filter) (err error)                      { return }
func (seg *TxnSegment) PushUpdateOp(handle.Filter, string, interface{}) (err error) { return }
func (seg *TxnSegment) CreateBlock() (blk handle.Block, err error)                  { return }

func (blk *TxnBlock) ID() uint64                                                    { return 0 }
func (blk *TxnBlock) String() string                                                { return "" }
func (blk *TxnBlock) Close() error                                                  { return nil }
func (blk *TxnBlock) GetByFilter(handle.Filter, bool) (bat *batch.Batch, err error) { return }
func (blk *TxnBlock) GetBatch(ctx interface{}) (bat *batch.Batch, err error)        { return }

func (blk *TxnBlock) Append(*batch.Batch, uint32) (n uint32, err error)           { return }
func (blk *TxnBlock) Update(uint32, uint16, interface{}) (err error)              { return }
func (blk *TxnBlock) RangeDelete(uint32, uint32) (err error)                      { return }
func (blk *TxnBlock) PushDeleteOp(handle.Filter) (err error)                      { return }
func (blk *TxnBlock) PushUpdateOp(handle.Filter, string, interface{}) (err error) { return }
