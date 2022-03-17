package txn

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
	"github.com/sirupsen/logrus"
)

type TableIndex interface {
	Insert([]byte, uint32) error
	Delete([]byte)
}

type Table struct {
	inodes     []InsertNode
	appendable InsertNode
	driver     NodeDriver
	id         uint64
	nodesMgr   base.INodeManager
	index      TableIndex
}

func NewTable(id uint64, driver NodeDriver, mgr base.INodeManager) *Table {
	tbl := &Table{
		inodes:   make([]InsertNode, 0),
		nodesMgr: mgr,
		id:       id,
		driver:   driver,
	}
	return tbl
}

func (tbl *Table) registerInsertNode() error {
	id := common.ID{
		TableID:   tbl.id,
		SegmentID: uint64(len(tbl.inodes)),
	}
	n := NewInsertNode(tbl.nodesMgr, id, tbl.driver)
	tbl.appendable = n
	tbl.inodes = append(tbl.inodes, n)
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
		h := tbl.nodesMgr.Pin(tbl.appendable)
		if h == nil {
			panic("unexpected")
		}
		defer h.Close()
		err := tbl.appendable.Expand(common.K, func() error {
			appended, err = tbl.appendable.Append(data, offset)
			return err
		})
		if err != nil {
			break
		}
		offset += appended
		space := tbl.appendable.GetSpace()
		logrus.Infof("Appended: %d, Space:%d", appended, space)
		if space == 0 {
			if err = tbl.registerInsertNode(); err != nil {
				break
			}
		}
		if offset >= length {
			break
		}
	}
	return err
}

// 1. Split the interval to multiple intervals, with each interval belongs to only one insert node
// 2. For each new interval, call insert node DeleteRows
// 3. Update the table index
func (tbl *Table) DeleteRows(interval *common.Range) error {
	first := int(interval.Left) / int(MaxNodeRows)
	firstOffset := interval.Left % uint64(MaxNodeRows)
	last := int(interval.Right) / int(MaxNodeRows)
	lastOffset := interval.Right % uint64(MaxNodeRows)
	var err error
	if last == first {
		node := tbl.inodes[first]
		err = node.DeleteRows(&common.Range{
			Left:  firstOffset,
			Right: lastOffset,
		})
	} else {
		node := tbl.inodes[first]
		err = node.DeleteRows(&common.Range{
			Left:  firstOffset,
			Right: uint64(MaxNodeRows) - 1,
		})
		node = tbl.inodes[last]
		err = node.DeleteRows(&common.Range{
			Left:  0,
			Right: lastOffset,
		})
		if last > first+1 {
			for i := first + 1; i < last; i++ {
				node = tbl.inodes[i]
				if err = node.DeleteRows(&common.Range{
					Left:  0,
					Right: uint64(MaxNodeRows),
				}); err != nil {
					break
				}
			}
		}
	}
	return err
}

func (tbl *Table) DebugLocalDeletes() string {
	s := fmt.Sprintf("<Table-%d>[LocalDeletes]:\n", tbl.id)
	for i, n := range tbl.inodes {
		s = fmt.Sprintf("%s\t<INode-%d>: %s\n", s, i, n.DebugDeletes())
	}
	return s
}

func (tbl *Table) IsLocalDeleted(row uint64) bool {
	npos := int(row) / int(MaxNodeRows)
	noffset := uint32(row % uint64(MaxNodeRows))
	n := tbl.inodes[npos]
	return n.IsRowDeleted(noffset)
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
