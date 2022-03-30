package catalog

import (
	"fmt"
	"sync"
	"tae/pkg/iface/txnif"

	"github.com/sirupsen/logrus"
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

func NewDBEntry(catalog *Catalog, name string, txnCtx txnif.AsyncTxn) *DBEntry {
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
	return fmt.Sprintf("DB%s[name=%s]", e.BaseEntry2.String(), e.name)
}

func (e *DBEntry) txnGetNodeByNameLocked(name string, txnCtx txnif.AsyncTxn) *DLNode {
	node := e.nameNodes[name]
	if node == nil {
		return nil
	}
	return node.TxnGetTableNodeLocked(txnCtx)
}

func (e *DBEntry) GetTableEntry(name string, txnCtx txnif.AsyncTxn) (entry *TableEntry, err error) {
	e.RLock()
	n := e.txnGetNodeByNameLocked(name, txnCtx)
	e.RUnlock()
	if n == nil {
		return nil, ErrNotFound
	}
	entry = n.payload.(*TableEntry)
	return
}

func (e *DBEntry) DropTableEntry(name string, txnCtx txnif.AsyncTxn) (deleted *TableEntry, err error) {
	e.Lock()
	defer e.Unlock()
	dn := e.txnGetNodeByNameLocked(name, txnCtx)
	if dn == nil {
		err = ErrNotFound
		return
	}
	entry := dn.payload.(*TableEntry)
	entry.Lock()
	defer entry.Unlock()
	err = entry.DropEntryLocked(txnCtx)
	if err == nil {
		deleted = entry
	}
	return
}

func (e *DBEntry) CreateTableEntry(schema *Schema, txnCtx txnif.AsyncTxn) (created *TableEntry, err error) {
	e.Lock()
	old := e.txnGetNodeByNameLocked(schema.Name, txnCtx)
	if old != nil {
		oldE := old.payload.(*TableEntry)
		oldE.RLock()
		defer oldE.RUnlock()
		if oldE.Txn != nil {
			if oldE.Txn.GetID() == txnCtx.GetID() {
				if !oldE.IsDroppedUncommitted() {
					err = ErrDuplicate
				}
			} else {
				err = txnif.TxnWWConflictErr
			}
		} else {
			if !oldE.HasDropped() {
				err = ErrDuplicate
			}
		}
		if err != nil {
			e.Unlock()
			return
		}
	}
	created = NewTableEntry(e, schema, txnCtx)
	err = e.addEntryLocked(created, e.RWMutex)
	e.Unlock()

	return created, err
}

func (e *DBEntry) addEntryLocked(table *TableEntry, lockCtx sync.Locker) (err error) {
	var w Waitable
	for {
		w, err = e.tryAddEntryLocked(table)
		if w == nil {
			break
		}
		if lockCtx != nil {
			lockCtx.Unlock()
		}
		err = w.Wait()
		if lockCtx != nil {
			lockCtx.Lock()
		}
		if err != nil {
			break
		}
	}
	return
}

func (e *DBEntry) tryAddEntryLocked(table *TableEntry) (Waitable, error) {
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
		oldE.RLock()
		if oldE.HasActiveTxn() {
			oeTxn := oldE.Txn
			if oldE.IsSameTxn(table.Txn) {
				if !oldE.IsDroppedUncommitted() {
					oldE.RUnlock()
					return nil, ErrDuplicate
				}
			} else {
				if !oldE.IsCommitting() {
					oldE.RUnlock()
					return nil, txnif.TxnWWConflictErr
				}
				if oeTxn.GetCommitTS() < table.Txn.GetStartTS() {
					// if table.CreateAfter(oeTxn.GetCommitTS()) {
					nTxn := table.Txn
					oldE.RUnlock()
					return &waitable{
						fn: func() error {
							logrus.Infof("%s ----WAIT---->%s", nTxn.String(), oeTxn.String())
							oeTxn.GetTxnState(true)
							return nil
						},
					}, nil
				}
			}
		} else {
			if !oldE.HasDropped() {
				oldE.RUnlock()
				return nil, ErrDuplicate
			}
		}
		oldE.RUnlock()
		n := e.link.Insert(table)
		e.entries[table.GetID()] = n
		nn.CreateNode(table.GetID())
	}
	return nil, nil
}
