package txn

import (
	"bytes"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/stretchr/testify/assert"
)

func TestPointerCmd(t *testing.T) {
	groups := uint32(10)
	maxLsn := uint64(10)
	// cmds := make([]TxnCmd, int(groups)*int(maxLsn))
	// mashalled := make([][]byte, int(groups)*int(maxLsn))
	for group := uint32(1); group <= groups; group++ {
		for lsn := uint64(1); lsn <= maxLsn; lsn++ {
			cmd := new(PointerCmd)
			cmd.Group = group
			cmd.Lsn = lsn
			mashalled, err := cmd.Marshal()
			assert.Nil(t, err)
			r := bytes.NewBuffer(mashalled)
			cmd2, err := BuildCommandFrom(r)
			assert.Nil(t, err)
			assert.Equal(t, cmd.Group, cmd2.(*PointerCmd).Group)
			assert.Equal(t, cmd.Lsn, cmd2.(*PointerCmd).Lsn)
		}
	}
}

// func TestDeletesCmd(t *testing.T) {
// 	deletes := make(map[uint32]*roaring64.Bitmap)
// 	for i := 0; i < 10; i++ {
// 		deletes[uint32(i)] = roaring64.NewBitmap()
// 		deletes[uint32(i)].Add(uint64(i)*2 + 1)
// 	}
// 	cmd := MakeDeletesCmd(deletes)
// 	var w bytes.Buffer
// 	err := cmd.WriteTo(&w)
// 	assert.Nil(t, err)

// 	buf := w.Bytes()
// 	r := bytes.NewBuffer(buf)
// 	cmd2, err := BuildCommandFrom(r)
// 	assert.Nil(t, err)
// 	for k, v := range cmd2.(*LocalDeletesCmd).Deletes {
// 		assert.True(t, v.Contains(2*uint64(k)+1))
// 	}
// }

func TestComposedCmd(t *testing.T) {
	composed := NewComposedCmd()
	groups := uint32(10)
	maxLsn := uint64(10)
	// pts := make([]TxnCmd, int(groups)*int(maxLsn))
	for group := uint32(1); group <= groups; group++ {
		for lsn := uint64(1); lsn <= maxLsn; lsn++ {
			cmd := new(PointerCmd)
			cmd.Group = group
			cmd.Lsn = lsn
			composed.AddCmd(cmd)
			// pts = append(pts, cmd)
		}
	}
	batCnt := 5

	schema := metadata.MockSchema(4)
	for i := 0; i < batCnt; i++ {
		data := mock.MockBatch(schema.Types(), (uint64(i)+1)*5)
		bat, err := CopyToIBatch(data)
		assert.Nil(t, err)
		batCmd := NewBatchCmd(bat, schema.Types())
		del := roaring64.NewBitmap()
		del.Add(uint64(i))
		delCmd := NewDeleteBitmapCmd(del)
		comp := NewComposedCmd()
		comp.AddCmd(batCmd)
		comp.AddCmd(delCmd)
		composed.AddCmd(comp)
	}
	var w bytes.Buffer
	err := composed.WriteTo(&w)
	assert.Nil(t, err)

	buf := w.Bytes()

	r := bytes.NewBuffer(buf)
	composed2, err := BuildCommandFrom(r)
	assert.Nil(t, err)
	cmd1 := composed.Cmds
	cmd2 := composed2.(*ComposedCmd).Cmds

	assert.Equal(t, len(cmd1), len(cmd2))
	for i, c1 := range cmd1 {
		c2 := cmd2[i]
		assert.Equal(t, c1.GetType(), c2.GetType())
		switch c1.GetType() {
		case CmdPointer:
			assert.Equal(t, c1.(*PointerCmd).Group, c2.(*PointerCmd).Group)
			assert.Equal(t, c1.(*PointerCmd).Group, c2.(*PointerCmd).Group)
		case CmdComposed:
			comp1 := c1.(*ComposedCmd)
			comp2 := c2.(*ComposedCmd)
			for j, cc1 := range comp1.Cmds {
				cc2 := comp2.Cmds[j]
				assert.Equal(t, cc1.GetType(), cc2.GetType())
				switch cc1.GetType() {
				case CmdPointer:
					assert.Equal(t, cc1.(*PointerCmd).Group, cc2.(*PointerCmd).Group)
					assert.Equal(t, cc1.(*PointerCmd).Group, cc2.(*PointerCmd).Group)
				case CmdDeleteBitmap:
					assert.True(t, cc1.(*DeleteBitmapCmd).Bitmap.Equals(cc1.(*DeleteBitmapCmd).Bitmap))
				case CmdBatch:
					b1 := cc1.(*BatchCmd)
					b2 := cc2.(*BatchCmd)
					assert.Equal(t, b1.Types, b2.Types)
					assert.Equal(t, b1.Bat.Length(), b2.Bat.Length())
				}
			}
		}
	}
}
