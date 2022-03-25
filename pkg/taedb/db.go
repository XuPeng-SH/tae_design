package taedb

import (
	"tae/pkg/iface"
	"tae/pkg/txn"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type TxnCtx interface {
	GetID() uint64
}

type TAE interface {
	// TODO: DB should be specified during StartTxn
	StartTxn() (TxnCtx, error)
	CommitTxn(TxnCtx) error
	RollbackTxn(TxnCtx) error

	CreateTable(desc *CreateTableDesc, txnCtx TxnCtx) (uint64, error)
	DropTable(desc *DropTableDesc, txnCtx TxnCtx) (uint64, error)

	AppendRows(desc *AppendDesc, txnCtx TxnCtx) error
	BatchDedup(desc *BatchDedupDesc, txnCtx TxnCtx) error

	GetByFilter(desc *FilterDesc, txnCtx TxnCtx) (*batch.Batch, error)
	DeleteByFilter(desc *FilterDesc, txnCtx TxnCtx) error
	UpdateByFilter(desc *UpdateDesc, txnCtx TxnCtx) error

	DeleteRows(desc *DeleteRowsDesc, txnCtx TxnCtx) error
}

type taeTnxCtx struct {
	id uint64
}

func (ctx *taeTnxCtx) GetID() uint64 { return ctx.id }

type tae struct {
	TxnMgr *txn.TxnManager
}

func (db *tae) GetTxn(ctx TxnCtx) (iface.AsyncTxn, error) {
	transaction := db.TxnMgr.GetTxn(ctx.GetID())
	if transaction == nil {
		return nil, ErrTxnNotFound
	}
	return transaction, nil
}

func (db *tae) StartTxn() (TxnCtx, error) {
	ctx := db.TxnMgr.StartTxn(nil)
	return ctx, nil
}

func (db *tae) CommitTxn(ctx TxnCtx) error {
	transaction, err := db.GetTxn(ctx)
	if err != nil {
		return err
	}
	return transaction.Commit()
}

func (db *tae) RollbackTxn(ctx TxnCtx) error {
	transaction, err := db.GetTxn(ctx)
	if err != nil {
		return err
	}
	return transaction.Rollback()
}

func (db *tae) AppendRows(desc *AppendDesc, ctx TxnCtx) error {
	// transaction, err := db.GetTxn(ctx)
	// if err != nil {
	// 	return err
	// }
	// entry, err := db.getOrSetTable(transaction, desc.DB, desc.Table)
	// if err != nil {
	// 	return err
	// }
	// if err = entry.Store.BatchDedup(desc.Data, transaction); err != nil {
	// 	return err
	// }
	// if err = transaction.BatchDedup(desc.Data); err != nil {
	//  return err
	// }

	// return transaction.AppendRows(desc.Data)
	return nil
}

func (db *tae) getOrSetTable(transaction iface.AsyncTxn, dbName, tableName string) *metadata.Table {
	// entry := transaction.GetTableByName(desc.DB, desc.Table)
	// if entry == nil {
	// 	if entry, err = db.Catalog.GetTableByName(desc.DB, desc.Table, transaction); err != nil {
	// 		return err
	// 	}
	// 	transaction.RegisterTable(entry)
	// }
	// return entry
	return nil
}

func (db *tae) BatchDedup(desc *BatchDedupDesc, ctx TxnCtx) error {
	// transaction, err := db.GetTxn(ctx)
	// if err != nil {
	// 	return err
	// }
	// entry := db.getOrSetTable(transaction, desc.DB, desc.Table)
	// if err = entry.Store.BatchDedup(desc.Col, transaction); err != nil {
	// 	return err
	// }
	// return transaction.BatchDedup(desc.Col)
	return nil
}
