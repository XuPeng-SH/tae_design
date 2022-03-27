package catalog

import (
	"sync"
	"tae/pkg/iface"
	"tae/pkg/txn"

	"github.com/jiangxinmeng1/logstore/pkg/store"
)

// +--------+---------+----------+----------+------------+
// |   ID   |  Name   | CreateAt | DeleteAt | CommitInfo |
// +--------+---------+----------+----------+------------+
// |(uint64)|(varchar)| (uint64) | (uint64) |  (varchar) |
// +--------+---------+----------+----------+------------+
type Catalog struct {
	*IDAlloctor
	// sm.ClosedState
	// sm.StateMachine
	*sync.RWMutex
	store store.Store

	entries   map[uint64]*DLNode
	nameNodes map[string]*nodeList
	link      *Link

	nodesMu  sync.RWMutex
	commitMu sync.RWMutex
}

func MockCatalog(dir, name string, cfg *store.StoreCfg) *Catalog {
	driver, err := store.NewBaseStore(dir, name, cfg)
	if err != nil {
		panic(err)
	}
	catalog := &Catalog{
		RWMutex:    new(sync.RWMutex),
		IDAlloctor: NewIDAllocator(),
		store:      driver,
		entries:    make(map[uint64]*DLNode),
		nameNodes:  make(map[string]*nodeList),
		link:       new(Link),
	}
	// catalog.StateMachine.Start()
	return catalog
}

func (catalog *Catalog) Close() error {
	// catalog.Stop()
	return catalog.store.Close()
}

func (catalog *Catalog) addEntryLocked(database *DBEntry) error {
	nn := catalog.nameNodes[database.name]
	if nn == nil {
		n := catalog.link.Insert(database)
		catalog.entries[database.GetID()] = n

		nn := newNodeList(catalog, &catalog.nodesMu, database.name)
		catalog.nameNodes[database.name] = nn

		nn.CreateNode(database.GetID())
	} else {
		old := nn.GetDBNode()
		oldE := old.payload.(*DBEntry)
		if database.IsSameTxn(oldE.BaseEntry.CreateStartTS) || database.IsSameTxn(oldE.BaseEntry.DropStartTS) {
			if !oldE.IsDroppedUncommitted() {
				return ErrDuplicate
			}
		} else if !oldE.HasStarted() {
			return txn.TxnWWConflictErr
		} else if !oldE.IsDroppedCommitted() {
			return ErrDuplicate
		}
		n := catalog.link.Insert(database)
		catalog.entries[database.GetID()] = n
		nn.CreateNode(database.GetID())
	}
	return nil
}

func (catalog *Catalog) removeEntryLocked(database *DBEntry) error {
	if n, ok := catalog.entries[database.GetID()]; !ok {
		return ErrNotFound
	} else {
		nn := catalog.nameNodes[database.name]
		nn.DeleteNode(database.GetID())
		catalog.link.Delete(n)
	}
	return nil
}

func (catalog *Catalog) txnGetNodeByNameLocked(name string, txnCtx iface.TxnReader) *DLNode {
	node := catalog.nameNodes[name]
	if node == nil {
		return nil
	}
	return node.TxnGetDBNodeLocked(txnCtx)
}

func (catalog *Catalog) GetDBEntry(name string, txnCtx iface.TxnReader) (*DBEntry, error) {
	catalog.RLock()
	n := catalog.txnGetNodeByNameLocked(name, txnCtx)
	catalog.RUnlock()
	if n == nil {
		return nil, ErrNotFound
	}
	return n.payload.(*DBEntry), nil
}

func (catalog *Catalog) DropDBEntry(name string, txnCtx iface.TxnReader) (deleted *DBEntry, err error) {
	catalog.Lock()
	defer catalog.Unlock()
	dn := catalog.txnGetNodeByNameLocked(name, txnCtx)
	if dn == nil {
		err = ErrNotFound
		return
	}
	entry := dn.payload.(*DBEntry)
	err = entry.DropEntryLocked(txnCtx)
	if err == nil {
		deleted = entry
	}
	return
}

func (catalog *Catalog) CreateDBEntry(name string, txnCtx iface.TxnReader) (*DBEntry, error) {
	var err error
	catalog.RLock()
	old := catalog.txnGetNodeByNameLocked(name, txnCtx)
	if old != nil {
		oldE := old.payload.(*DBEntry)
		if oldE.IsSameTxn(txnCtx.GetStartTS()) {
			if !oldE.IsDroppedUncommitted() {
				err = ErrDuplicate
			}
		} else if !oldE.HasStarted() {
			err = txn.TxnWWConflictErr
		} else if !oldE.IsDroppedCommitted() {
			err = ErrDuplicate
		}
		catalog.RUnlock()
		if err != nil {
			return nil, err
		}
	} else {
		catalog.RUnlock()
	}
	entry := NewDBEntry(catalog, name, txnCtx)
	catalog.Lock()
	defer catalog.Unlock()
	return entry, catalog.addEntryLocked(entry)
}
