package demo

type CmdScope int8

const (
	CScopeCatalog CmdScope = iota
	CScopeTable
)

type Transaction struct{} // txnbase.TxnCtx

type CmdNode struct {
	Txn   *Transaction
	Cmd   []byte
	Scope CmdScope
	ID    *TableID
}

type TxnCommandMgr interface {
	AddCmdNode(txn *Transaction,
		scope CmdScope,
		id *TableID) (*CmdNode, error)
	CmdsInRange(
		startTs Timestamp,
		endTs Timestamp,
		scope CmdScope,
		id *TableID) (nodes *CmdNode, waiters []int, err error)
	DeleteTill(ts Timestamp) (err error)
}
