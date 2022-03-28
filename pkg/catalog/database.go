package catalog

import (
	"fmt"
	"sync"
	"tae/pkg/iface"
	"tae/pkg/txn"
)

type DBEntry struct {
	*BaseEntry
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
		BaseEntry: &BaseEntry{
			CommitInfo: CommitInfo{
				CreateStartTS:  txnCtx.GetStartTS(),
				CreateCommitTS: UncommitTS,
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
	oe := o.(*DBEntry).BaseEntry
	return e.DoCompre(oe)
}

func (e *DBEntry) String() string {
	s := fmt.Sprintf("DB<%d>[\"%s\"]: [%d-%d],[%d-%d]", e.ID, e.name, e.CreateStartTS, e.CreateCommitTS, e.DropStartTS, e.DropCommitTS)
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
		if oldE.IsSameTxn(txnCtx.GetStartTS()) {
			if !oldE.IsDroppedUncommitted() {
				err = ErrDuplicate
			}
		} else if !oldE.HasStarted() {
			err = txn.TxnWWConflictErr
		} else if !oldE.IsDroppedCommitted() {
			err = ErrDuplicate
		}
		if err != nil {
			return nil, err
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
		old := nn.GetDBNode()
		oldE := old.payload.(*DBEntry)
		if table.IsSameTxn(oldE.BaseEntry.CreateStartTS) || table.IsSameTxn(oldE.BaseEntry.DropStartTS) {
			if !oldE.IsDroppedUncommitted() {
				return ErrDuplicate
			}
		} else if !oldE.HasStarted() {
			return txn.TxnWWConflictErr
		} else if !oldE.IsDroppedCommitted() {
			return ErrDuplicate
		}
		n := e.link.Insert(table)
		e.entries[table.GetID()] = n
		nn.CreateNode(table.GetID())
	}
	return nil
}
