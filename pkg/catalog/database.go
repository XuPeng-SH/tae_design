package catalog

import (
	"fmt"
	"sync"
	"tae/pkg/iface"
)

type DBEntry struct {
	*BaseEntry
	catalog *Catalog
	name    string
	// entries map[uint64]*TableEntry
	// nameNodes map[string]*nodeList
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
		catalog: catalog,
		name:    name,
		// entries: make(map[uint64])
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
