package txn

import (
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
)

type CommandIndex struct {
	Id     uint64
	Offset uint32
	Size   uint32
}

// TODO: No copy needed later. Here just a prototype demo
func PreprocessAppendCmd(cmd *AppendCmd) (cid *CommandIndex, bat *gbat.Batch, err error) {
	// Read from log
	if cmd.Node == nil {
		// TODO
		return
	}
	// Read from node
	// TODO
	return
}
