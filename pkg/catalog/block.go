package catalog

import (
	"fmt"
	"sync"
	com "tae/pkg/common"
	"tae/pkg/iface/txnif"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type BlockEntry struct {
	*BaseEntry
	segment *SegmentEntry
	state   EntryState
}

func NewBlockEntry(segment *SegmentEntry, txn txnif.AsyncTxn, state EntryState) *BlockEntry {
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
		state:   state,
	}
	return e
}

func (entry *BlockEntry) IsAppendable() bool {
	return entry.state == ES_Appendable
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

func (entry *BlockEntry) PPString(level com.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", com.RepeatStr("\t", depth), prefix, entry.StringLocked())
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

func (entry *BlockEntry) AsCommonID() *common.ID {
	return &common.ID{
		TableID:   entry.GetSegment().GetTable().GetID(),
		SegmentID: entry.GetSegment().GetID(),
		BlockID:   entry.GetID(),
	}
}
