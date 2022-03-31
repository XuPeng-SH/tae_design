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

func (rel *TxnRelation) String() string                      { return "" }
func (rel *TxnRelation) Close() error                        { return nil }
func (rel *TxnRelation) ID() uint64                          { return 0 }
func (rel *TxnRelation) Rows() int64                         { return 0 }
func (rel *TxnRelation) Size(attr string) int64              { return 0 }
func (rel *TxnRelation) GetCardinality(attr string) int64    { return 0 }
func (rel *TxnRelation) Schema() interface{}                 { return nil }
func (rel *TxnRelation) MakeSegmentIt() handle.SegmentIt     { return nil }
func (rel *TxnRelation) MakeReader() handle.Reader           { return nil }
func (rel *TxnRelation) BatchDedup(col *vector.Vector) error { return nil }
func (rel *TxnRelation) Append(data *batch.Batch) error      { return nil }
func (rel *TxnRelation) GetMeta() interface{}                { return nil }
