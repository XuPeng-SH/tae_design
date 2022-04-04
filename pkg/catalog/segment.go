package catalog

import (
	"fmt"
	"sync"
	"tae/pkg/common"
	"tae/pkg/iface/txnif"
)

type SegmentEntry struct {
	*BaseEntry
	table   *TableEntry
	entries map[uint64]*DLNode
	link    *Link
}

func NewSegmentEntry(table *TableEntry, txn txnif.AsyncTxn) *SegmentEntry {
	id := table.GetDB().catalog.NextSegment()
	e := &SegmentEntry{
		BaseEntry: &BaseEntry{
			CommitInfo: CommitInfo{
				Txn:    txn,
				CurrOp: OpCreate,
			},
			RWMutex: new(sync.RWMutex),
			ID:      id,
		},
		table:   table,
		link:    new(Link),
		entries: make(map[uint64]*DLNode),
	}
	return e
}

func (entry *SegmentEntry) GetBlockEntryByID(id uint64) (blk *BlockEntry, err error) {
	entry.RLock()
	defer entry.RUnlock()
	node := entry.entries[id]
	if node == nil {
		err = ErrNotFound
		return
	}
	blk = node.payload.(*BlockEntry)
	return
}

func (entry *SegmentEntry) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmdType := CmdCreateSegment
	entry.RLock()
	defer entry.RUnlock()
	if entry.CurrOp == OpSoftDelete {
		cmdType = CmdDropSegment
	}
	return newSegmentCmd(id, cmdType, entry), nil
}

func (entry *SegmentEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, entry.StringLocked())
	if level == common.PPL0 {
		return s
	}
	var body string
	it := entry.MakeBlockIt(true)
	for it.Valid() {
		block := it.curr.payload.(*BlockEntry)
		if len(body) == 0 {
			body = block.PPString(level, depth+1, prefix)
		} else {
			body = fmt.Sprintf("%s\n%s", body, block.PPString(level, depth+1, prefix))
		}
		it.Next()
	}
	if len(body) == 0 {
		return s
	}
	return fmt.Sprintf("%s\n%s", s, body)
}

func (entry *SegmentEntry) StringLocked() string {
	return fmt.Sprintf("SEGMENT%s", entry.BaseEntry.String())
}

func (entry *SegmentEntry) String() string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringLocked()
}

func (entry *SegmentEntry) GetTable() *TableEntry {
	return entry.table
}

func (entry *SegmentEntry) Compare(o NodePayload) int {
	oe := o.(*SegmentEntry).BaseEntry
	return entry.DoCompre(oe)
}

func (entry *SegmentEntry) CreateBlock(txn txnif.AsyncTxn) (created *BlockEntry, err error) {
	entry.Lock()
	defer entry.Unlock()
	created = NewBlockEntry(entry, txn)
	entry.addEntryLocked(created)
	return
}

func (entry *SegmentEntry) MakeBlockIt(reverse bool) *LinkIt {
	return NewLinkIt(entry.RWMutex, entry.link, reverse)
}

func (entry *SegmentEntry) addEntryLocked(block *BlockEntry) {
	n := entry.link.Insert(block)
	entry.entries[block.GetID()] = n
}
