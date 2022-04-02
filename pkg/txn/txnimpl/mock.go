package txnimpl

import (
	"fmt"
	"tae/pkg/catalog"
	"tae/pkg/txn/txnbase"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
)

type mockRelation struct {
	*txnbase.TxnRelation
	entry *catalog.TableEntry
	id    uint64
}

func mockTestRelation(id uint64, schema *catalog.Schema) *mockRelation {
	entry := catalog.MockStaloneTableEntry(id, schema)
	return &mockRelation{
		TxnRelation: new(txnbase.TxnRelation),
		id:          id,
		entry:       entry,
	}
}

func (rel *mockRelation) GetID() uint64        { return rel.id }
func (rel *mockRelation) GetMeta() interface{} { return rel.entry }

type mockBlocks struct {
	schema       *catalog.Schema
	maxRowsBlock int
	blks         []*mockBlock
	t            *testing.T
}

func NewMockBlocks(t *testing.T, schema *catalog.Schema, maxRows int) *mockBlocks {
	seg := &mockBlocks{
		schema:       schema,
		maxRowsBlock: maxRows, //instead of schema.BlockMaxRow
		blks:         make([]*mockBlock, 0),
		t:            t,
	}
	seg.CreateBlock()
	return seg
}

func (seg *mockBlocks) getAttrs() []string {
	attrs := make([]string, 0)
	for _, col := range seg.schema.ColDefs {
		attrs = append(attrs, col.Name)
	}
	return attrs
}

func (seg *mockBlocks) CreateBlock() {
	blk := &mockBlock{
		seg:  seg,
		size: 0,
		t:    seg.t,
	}
	seg.blks = append(seg.blks, blk)
}

func (seg *mockBlocks) GetBlock() *mockBlock {
	return seg.blks[len(seg.blks)-1]
}

type mockBlock struct {
	seg  *mockBlocks
	size int
	t    *testing.T
}

func (blk *mockBlock) PrepareAppend(data batch.IBatch, offset uint32) (n uint32, err error, info []byte) {
	length := data.Length()-int(offset)
	blk.t.Logf("<%p>%d/%d prepare append %d rows", blk, blk.size, blk.seg.maxRowsBlock, length)
	if length+blk.size > blk.seg.maxRowsBlock {
		n = uint32(blk.seg.maxRowsBlock - blk.size)
	} else {
		n = uint32(length)
	}
	blk.size += int(n)
	infoStr := fmt.Sprintf("<%p>%d/%d append %d rows", blk, blk.size, blk.seg.maxRowsBlock, n)
	blk.t.Logf("%s", infoStr)
	info = []byte(infoStr)
	return
}
