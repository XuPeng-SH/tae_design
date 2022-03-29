package catalog

import (
	"sync"
	"tae/pkg/iface"
	"tae/pkg/iface/txnif"

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

func (catalog *Catalog) addEntryLocked(database *DBEntry) (Waitable, error) {
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
		oldE.RLock()
		if !oldE.HasActiveTxn() {
			if !oldE.HasDropped() {
				oldE.RUnlock()
				return nil, ErrDuplicate
			}
		} else {
			eTxn := oldE.Txn
			if oldE.IsSameTxn(database.Txn) {
				if !oldE.IsDroppedUncommitted() {
					oldE.RUnlock()
					return nil, ErrDuplicate
				}
			} else {
				if !oldE.IsCommitting() {
					oldE.RUnlock()
					return nil, txnif.TxnWWConflictErr
				}
				if oldE.Txn.GetCommitTS() < database.Txn.GetStartTS() {
					oldE.RUnlock()
					return &waitable{fn: func() error {
						// oldE.RLock()
						// txn := oldE.Txn
						// oldE.RUnlock()
						eTxn.GetTxnState(true)
						return nil
					}}, nil
				}
			}
		}
		oldE.RUnlock()
		n := catalog.link.Insert(database)
		catalog.entries[database.GetID()] = n
		nn.CreateNode(database.GetID())
	}
	return nil, nil
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

func (catalog *Catalog) txnGetNodeByNameLocked(name string, txnCtx txnif.AsyncTxn) *DLNode {
	node := catalog.nameNodes[name]
	if node == nil {
		return nil
	}
	return node.TxnGetDBNodeLocked(txnCtx)
}

func (catalog *Catalog) GetDBEntry(name string, txnCtx txnif.AsyncTxn) (*DBEntry, error) {
	catalog.RLock()
	n := catalog.txnGetNodeByNameLocked(name, txnCtx)
	catalog.RUnlock()
	if n == nil {
		return nil, ErrNotFound
	}
	return n.payload.(*DBEntry), nil
}

func (catalog *Catalog) DropDBEntry(name string, txnCtx txnif.AsyncTxn) (deleted *DBEntry, err error) {
	catalog.Lock()
	defer catalog.Unlock()
	dn := catalog.txnGetNodeByNameLocked(name, txnCtx)
	if dn == nil {
		err = ErrNotFound
		return
	}
	entry := dn.payload.(*DBEntry)
	entry.Lock()
	defer entry.Unlock()
	err = entry.DropEntryLocked(txnCtx)
	if err == nil {
		deleted = entry
	}
	return
}

func (catalog *Catalog) CreateDBEntry(name string, txnCtx txnif.AsyncTxn) (*DBEntry, error) {
	var err error
	catalog.Lock()
	old := catalog.txnGetNodeByNameLocked(name, txnCtx)
	if old != nil {
		oldE := old.payload.(*DBEntry)
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
			catalog.Unlock()
			return nil, err
		}
	}
	entry := NewDBEntry(catalog, name, txnCtx)
	var w Waitable
	for {
		w, err = catalog.addEntryLocked(entry)
		if w == nil {
			break
		}
		catalog.Unlock()
		err = w.Wait()
		catalog.Lock()
		if err != nil {
			break
		}
	}
	catalog.Unlock()

	return entry, err
}

func (catalog *Catalog) MakeDBHandle(txnCtx txnif.AsyncTxn) iface.Database {
	return nil
}
