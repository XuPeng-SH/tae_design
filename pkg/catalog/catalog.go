package catalog

import (
	"sync"

	"github.com/jiangxinmeng1/logstore/pkg/store"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore/sm"
)

// +--------+---------+----------+----------+------------+
// |   ID   |  Name   | CreateAt | DeleteAt | CommitInfo |
// +--------+---------+----------+----------+------------+
// |(uint64)|(varchar)| (uint64) | (uint64) |  (varchar) |
// +--------+---------+----------+----------+------------+
type Catalog struct {
	*IDAlloctor
	sm.ClosedState
	sm.StateMachine
	*sync.RWMutex
	store store.Store

	entries   map[uint64]*DBEntry
	nameNodes map[string]*nodeList

	nodesMu  sync.RWMutex
	commitMu sync.RWMutex
}
