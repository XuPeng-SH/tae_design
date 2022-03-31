package txnimpl

import (
	"tae/pkg/catalog"
	"tae/pkg/iface/handle"
	"tae/pkg/iface/txnif"
	"tae/pkg/txn/txnbase"
)

type txnImpl struct {
	*txnbase.Txn
	catalog *catalog.Catalog
}

var TxnFactory = func(catalog *catalog.Catalog) txnbase.TxnFactory {
	return func(mgr *txnbase.TxnManager, store txnif.TxnStore, txnId, start uint64, info []byte) txnif.AsyncTxn {
		return newTxnImpl(catalog, mgr, store, txnId, start, info)
	}
}

func newTxnImpl(catalog *catalog.Catalog, mgr *txnbase.TxnManager, store txnif.TxnStore, txnId, start uint64, info []byte) *txnImpl {
	impl := &txnImpl{
		Txn:     txnbase.NewTxn(mgr, store, txnId, start, info),
		catalog: catalog,
	}
	return impl
}

func (txn *txnImpl) CreateDatabase(name string) (db handle.Database, err error) {
	return txn.Store.CreateDatabase(name)
}

func (txn *txnImpl) DropDatabase(name string) (db handle.Database, err error) {
	return
}

func (txn *txnImpl) GetDatabase(name string) (db handle.Database, err error) {
	return
}
