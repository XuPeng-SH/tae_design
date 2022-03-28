package catalog

import (
	"tae/pkg/iface"
	"tae/pkg/iface/txnif"
)

type dbTxnHandle struct {
	ctx txnif.TxnReader
	db  *DBEntry
}

func newDBTxnHandle(db *DBEntry, ctx txnif.TxnReader) *dbTxnHandle {
	return &dbTxnHandle{
		ctx: ctx,
		db:  db,
	}
}

func (h *dbTxnHandle) Relation(name string) (rel iface.Relation, err error) {
	return
}

func (h *dbTxnHandle) Relations() (names []string) {
	return
}

func (h *dbTxnHandle) Create(name string, def iface.ResourceDef) (rel iface.Relation, err error) {
	return
}

func (h *dbTxnHandle) Drop(name string) (rel iface.Relation, err error) {
	return
}
