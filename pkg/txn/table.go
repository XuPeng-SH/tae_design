package txn

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type TableIndex interface {
	Insert([]byte, uint32) error
	Delete([]byte)
}

type Table struct {
	nodes      []Node
	appendable InsertNode
	driver     NodeDriver
	id         uint64
	nodesMgr   base.INodeManager
	index      TableIndex
}

func (tbl *Table) registerInsertNode() error {
	id := common.ID{
		TableID: tbl.id,
	}
	n := NewInsertNode(tbl.nodesMgr, id, tbl.driver)
	tbl.appendable = n
	tbl.nodes = append(tbl.nodes, n)
	return nil
}

func (tbl *Table) Append(data *batch.Batch) error {
	var err error
	if tbl.appendable == nil {
		if err = tbl.registerInsertNode(); err != nil {
			return err
		}
	}
	appended := uint32(0)
	offset := uint32(0)
	length := uint32(vector.Length(data.Vecs[0]))
	for {
		if appended, err = tbl.appendable.Append(data, offset); err != nil {
			break
		}
		offset += appended
		if offset >= length {
			break
		}
		space := tbl.appendable.GetSpace()
		if space == 0 {
			if err = tbl.registerInsertNode(); err != nil {
				break
			}
		}
	}
	return err
}

func (tbl *Table) DeleteRows(interval *common.Range) error {
	// TODO
	// 1. Split the interval to multiple intervals, with each interval belongs to only one insert node
	// 2. For each new interval, call insert node DeleteRows
	// 3. Update the table index
	return nil
}

func (tbl *Table) UpdateValue(row uint32, col uint16, value interface{}) error {
	// TODO
	// 1. Get insert node and offset in node
	// 2. Get row
	// 3. Build a new row
	// 4. Delete the row in the node
	// 5. Append the new row
	return nil
}
