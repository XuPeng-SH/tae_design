package catalog

import (
	"fmt"
	"sync"
	com "tae/pkg/common"
	"tae/pkg/iface/data"
	"tae/pkg/iface/txnif"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type SegmentDataFactory = func(meta *SegmentEntry) data.Segment

type SegmentEntry struct {
	*BaseEntry
	table   *TableEntry
	entries map[uint64]*com.DLNode
	link    *com.Link
	state   EntryState
	segData data.Segment
}

func NewSegmentEntry(table *TableEntry, txn txnif.AsyncTxn, state EntryState) *SegmentEntry {
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
		link:    new(com.Link),
		entries: make(map[uint64]*com.DLNode),
		state:   state,
	}
	if dataFactory := table.GetCatalog().GetDataFactory(); dataFactory != nil {
		e.segData = dataFactory.Segment(e)
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
	blk = node.GetPayload().(*BlockEntry)
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

func (entry *SegmentEntry) PPString(level com.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", com.RepeatStr("\t", depth), prefix, entry.StringLocked())
	if level == com.PPL0 {
		return s
	}
	var body string
	it := entry.MakeBlockIt(true)
	for it.Valid() {
		block := it.Get().GetPayload().(*BlockEntry)
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

func (entry *SegmentEntry) IsAppendable() bool {
	return entry.state == ES_Appendable
}

func (entry *SegmentEntry) GetTable() *TableEntry {
	return entry.table
}

func (entry *SegmentEntry) Compare(o com.NodePayload) int {
	oe := o.(*SegmentEntry).BaseEntry
	return entry.DoCompre(oe)
}

func (entry *SegmentEntry) GetAppendableBlockCnt() int {
	cnt := 0
	it := entry.MakeBlockIt(true)
	for it.Valid() {
		if it.Get().GetPayload().(*BlockEntry).IsAppendable() {
			cnt++
		}
		it.Next()
	}
	return cnt
}

func (entry *SegmentEntry) LastAppendableBlock() (blk *BlockEntry) {
	it := entry.MakeBlockIt(false)
	for it.Valid() {
		itBlk := it.Get().GetPayload().(*BlockEntry)
		if itBlk.IsAppendable() {
			blk = itBlk
		}
		it.Next()
	}
	return blk
}

func (entry *SegmentEntry) CreateBlock(txn txnif.AsyncTxn, state EntryState) (created *BlockEntry, err error) {
	entry.Lock()
	defer entry.Unlock()
	created = NewBlockEntry(entry, txn, state)
	entry.addEntryLocked(created)
	return
}

func (entry *SegmentEntry) MakeBlockIt(reverse bool) *com.LinkIt {
	return com.NewLinkIt(entry.RWMutex, entry.link, reverse)
}

func (entry *SegmentEntry) addEntryLocked(block *BlockEntry) {
	n := entry.link.Insert(block)
	entry.entries[block.GetID()] = n
}

func (entry *SegmentEntry) AsCommonID() *common.ID {
	return &common.ID{
		TableID:   entry.GetTable().GetID(),
		SegmentID: entry.GetID(),
	}
}

func (entry *SegmentEntry) GetCatalog() *Catalog { return entry.table.db.catalog }
