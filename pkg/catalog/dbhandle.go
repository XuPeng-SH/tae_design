package catalog

import "tae/pkg/iface"

type dbTxnHandle struct {
	ctx iface.TxnReader
	db  *DBEntry
}

func newDBTxnHandle(db *DBEntry, ctx iface.TxnReader) *dbTxnHandle {
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
