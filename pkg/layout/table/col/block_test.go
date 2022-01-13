package col

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlock(t *testing.T) {
	block := &Block{
		Nodes: make(map[int]*PartNode),
	}
	maxPos := 10
	block.Rows = NodeRows * maxPos
	assert.Equal(t, maxPos-1, block.MaxPos())
	block.Rows += NodeRows / 2
	assert.Equal(t, maxPos, block.MaxPos())
	assert.False(t, block.HasChange())

	it := block.NewIt()
	assert.True(t, it.Valid())
	t.Log(it.BaseNode.Start)
	t.Log(it.BaseNode.Count)
	it.Next()
	assert.False(t, it.Valid())

	block.AddNode(2)
	block.AddNode(4)

	it = block.NewIt()
	assert.True(t, it.Valid())
	assert.NotNil(t, it.BaseNode)
	assert.Equal(t, 2, it.BaseNode.Count)

	it.Next()
	assert.True(t, it.Valid())
	assert.NotNil(t, it.VersionNode)
	assert.Equal(t, 2, it.VersionNode.Pos)

	it.Next()
	assert.True(t, it.Valid())
	assert.NotNil(t, it.BaseNode)
	assert.Equal(t, 3, it.BaseNode.Start)
	assert.Equal(t, 1, it.BaseNode.Count)

	it.Next()
	assert.True(t, it.Valid())
	assert.NotNil(t, it.VersionNode)
	assert.Equal(t, 4, it.VersionNode.Pos)

	it.Next()
	assert.True(t, it.Valid())
	assert.NotNil(t, it.BaseNode)
	assert.Equal(t, 5, it.BaseNode.Start)
	assert.Equal(t, 6, it.BaseNode.Count)

	it.Next()
	assert.False(t, it.Valid())
}
