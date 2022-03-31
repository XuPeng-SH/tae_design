package txnimpl

import (
	"tae/pkg/catalog"
	"tae/pkg/iface/handle"
	"tae/pkg/iface/txnif"
	"tae/pkg/txn/txnbase"
)

type txnDatabase struct {
	*txnbase.TxnDatabase
	entry *catalog.DBEntry
}

func newDatabase(txn txnif.AsyncTxn, meta *catalog.DBEntry) *txnDatabase {
	db := &txnDatabase{
		TxnDatabase: &txnbase.TxnDatabase{
			Txn: txn,
		},
		entry: meta,
	}
	return db

}
func (db *txnDatabase) GetID() uint64   { return db.entry.GetID() }
func (db *txnDatabase) GetName() string { return db.entry.GetName() }
func (db *txnDatabase) String() string  { return db.entry.String() }

func (db *txnDatabase) CreateRelation(def interface{}) (rel handle.Relation, err error) {
	// schema := def.(*catalog.Schema)
	// meta, err := db.entry.CreateTableEntry(schema, db.Txn)
	// if err != nil {
	// 	return
	// }
	// db.Txn.GetStore().AddTxnEntry(TxnEntryCretaeTable, meta)
	// rel = newRelation(db.Txn, meta)
	return db.Txn.GetStore().CreateRelation(def)
}

func (db *txnDatabase) DropRelationByName(name string) (rel handle.Relation, err error) {
	meta, err := db.entry.DropTableEntry(name, db.Txn)
	if err != nil {
		return nil, err
	}
	db.Txn.GetStore().AddTxnEntry(TxnEntryDropTable, meta)
	rel = newRelation(db.Txn, meta)
	return
}

func (db *txnDatabase) GetRelationByName(name string) (rel handle.Relation, err error) { return }

func (db *txnDatabase) RelationCnt() int64                     { return 0 }
func (db *txnDatabase) Relations() (rels []handle.Relation)    { return }
func (db *txnDatabase) MakeRelationIt() (it handle.RelationIt) { return }
func (db *txnDatabase) Close() error                           { return nil }
func (db *txnDatabase) GetMeta() interface{}                   { return db.entry }
