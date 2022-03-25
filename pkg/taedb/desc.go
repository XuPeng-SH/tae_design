package taedb

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type CreateTableDesc struct {
	DB     string
	Schema *metadata.Schema
	Index  *metadata.IndexSchema
}

type DropTableDesc struct {
	DB   string
	Name string
}

type AppendDesc struct {
	DB    string
	Table string
	Data  *batch.Batch
	Dedup bool
}

type BatchDedupDesc struct {
	DB    string
	Table string
	Col   *vector.Vector
}

type FilterOp int16

const (
	FilterEq FilterOp = iota
	FilterBtw
)

type FilterDesc struct {
	Op         FilterOp
	ColOperand *vector.Vector
}

type UpdateDesc struct {
	Filter *FilterDesc
	Attr   string
	Value  interface{}
}

type DeleteRowsDesc struct {
	DB    string
	Table string
	Rows  *batch.Batch
}
