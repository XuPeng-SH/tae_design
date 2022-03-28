package catalog

import (
	"fmt"
	"sync"
	"tae/pkg/iface"
)

type DBEntry struct {
	// *BaseEntry
	*BaseEntry2
	catalog *Catalog
	name    string

	entries   map[uint64]*DLNode
	nameNodes map[string]*nodeList
	link      *Link

	nodesMu sync.RWMutex
}

func NewDBEntry(catalog *Catalog, name string, txnCtx iface.TxnReader) *DBEntry {
	id := catalog.NextDB()
	e := &DBEntry{
		BaseEntry2: &BaseEntry2{
			CommitInfo2: CommitInfo2{
				CurrOp: OpCreate,
				Txn:    txnCtx,
			},
			RWMutex: new(sync.RWMutex),
			ID:      id,
		},
		catalog:   catalog,
		name:      name,
		entries:   make(map[uint64]*DLNode),
		nameNodes: make(map[string]*nodeList),
		link:      new(Link),
	}
	return e
}

func (e *DBEntry) Compare(o NodePayload) int {
	oe := o.(*DBEntry).BaseEntry2
	return e.DoCompre(oe)
}

func (e *DBEntry) String() string {
	s := fmt.Sprintf("DB<%d>[\"%s\"]: [%d-%d]", e.ID, e.name, e.CreateAt, e.DeleteAt)
	if e.Txn != nil {
		s = fmt.Sprintf("%s, [%d-%d]", s, e.Txn.GetStartTS(), e.Txn.GetCommitTS())
	}
	return s
}

func (e *DBEntry) txnGetNodeByNameLocked(name string, txnCtx iface.TxnReader) *DLNode {
	node := e.nameNodes[name]
	if node == nil {
		return nil
	}
	return node.TxnGetTableNodeLocked(txnCtx)
}

func (e *DBEntry) GetTableEntry(name string, txnCtx iface.TxnReader) (entry *TableEntry, err error) {
	e.RLock()
	n := e.txnGetNodeByNameLocked(name, txnCtx)
	e.RUnlock()
	if n == nil {
		return nil, ErrNotFound
	}
	entry = n.payload.(*TableEntry)
	return
}

func (e *DBEntry) DropTableEntry(name string, txnCtx iface.TxnReader) (deleted *TableEntry, err error) {
	e.Lock()
	defer e.Unlock()
	dn := e.txnGetNodeByNameLocked(name, txnCtx)
	if dn == nil {
		err = ErrNotFound
		return
	}
	entry := dn.payload.(*TableEntry)
	err = entry.DropEntryLocked(txnCtx)
	if err == nil {
		deleted = entry
	}
	return
}

func (e *DBEntry) CreateTableEntry(schema *Schema, txnCtx iface.TxnReader) (created *TableEntry, err error) {
	e.Lock()
	defer e.Unlock()
	old := e.txnGetNodeByNameLocked(schema.Name, txnCtx)
	if old != nil {
		oldE := old.payload.(*TableEntry)
		if oldE.Txn != nil {
			if oldE.Txn.GetID() == txnCtx.GetID() {
				if !oldE.IsDroppedUncommitted() {
					err = ErrDuplicate
				}
			} else {
				err = iface.TxnWWConflictErr
			}
		} else {
			if !oldE.HasDropped() {
				err = ErrDuplicate
			}
		}
		if err != nil {
			return
		}
	}
	created = NewTableEntry(e, schema, txnCtx)

	return created, e.addEntryLocked(created)
}

func (e *DBEntry) addEntryLocked(table *TableEntry) error {
	nn := e.nameNodes[table.schema.Name]
	if nn == nil {
		n := e.link.Insert(table)
		e.entries[table.GetID()] = n

		nn := newNodeList(e, &e.nodesMu, table.schema.Name)
		e.nameNodes[table.schema.Name] = nn

		nn.CreateNode(table.GetID())
	} else {
		old := nn.GetTableNode()
		oldE := old.payload.(*TableEntry)
		if oldE.Txn == nil {
			if !oldE.HasDropped() {
				return ErrDuplicate
			}
		} else {
			if oldE.SameTxn(e.BaseEntry2) {
				if !oldE.IsDroppedUncommitted() {
					return ErrDuplicate
				}
			} else {
				return iface.TxnWWConflictErr
			}
		}
		n := e.link.Insert(table)
		e.entries[table.GetID()] = n
		nn.CreateNode(table.GetID())
	}
	return nil
}
