package catalog

import (
	"fmt"
	"sync"
	"tae/pkg/common"
	"tae/pkg/iface/txnif"
)

type BlockEntry struct {
	*BaseEntry
	segment *SegmentEntry
}

func NewBlockEntry(segment *SegmentEntry, txn txnif.AsyncTxn) *BlockEntry {
	id := segment.GetTable().GetDB().catalog.NextBlock()
	e := &BlockEntry{
		BaseEntry: &BaseEntry{
			CommitInfo: CommitInfo{
				Txn:    txn,
				CurrOp: OpCreate,
			},
			RWMutex: new(sync.RWMutex),
			ID:      id,
		},
		segment: segment,
	}
	return e
}

func (entry *BlockEntry) GetSegment() *SegmentEntry {
	return entry.segment
}

func (entry *BlockEntry) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmdType := CmdCreateBlock
	entry.RLock()
	defer entry.RUnlock()
	if entry.CurrOp == OpSoftDelete {
		cmdType = CmdDropBlock
	}
	return newBlockCmd(id, cmdType, entry), nil
}

func (entry *BlockEntry) Compare(o NodePayload) int {
	oe := o.(*BlockEntry).BaseEntry
	return entry.DoCompre(oe)
}

func (entry *BlockEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, entry.StringLocked())
	return s
}

func (entry *BlockEntry) String() string {
	entry.RLock()
	defer entry.RUnlock()
	return entry.StringLocked()
}

func (entry *BlockEntry) StringLocked() string {
	return fmt.Sprintf("BLOCK%s", entry.BaseEntry.String())
}
