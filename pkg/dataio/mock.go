package dataio

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
)

var SegmentFileMockFactory = func(dir string, id uint64) SegmentFile {
	return mockAppendableSegment(dir, id)
}

type mockBlockFile struct {
	NoopBlockFile
	id         uint64
	rows       uint32
	sorted     bool
	appendable bool
	segFile    SegmentFile
	maxTs      uint64
	data       batch.IBatch
}

type mockSegmentFile struct {
	NoopSegmentFile
	files      map[uint64]*mockBlockFile
	appendable bool
	sorted     bool
	name       string
}

func mockAppendableBlock(id uint64, bat batch.IBatch, segFile SegmentFile) *mockBlockFile {
	return &mockBlockFile{
		id:         id,
		appendable: true,
		segFile:    segFile,
		data:       bat,
	}
}

func mockAppendableSegment(dir string, id uint64) *mockSegmentFile {
	name := fmt.Sprintf("%s-mock-%d", dir, id)
	return &mockSegmentFile{
		files: make(map[uint64]*mockBlockFile),
		name:  name,
	}
}

func (bf *mockBlockFile) IsAppendable() bool { return bf.appendable }
func (bf *mockBlockFile) Rows() uint32 {
	if bf.appendable {
		panic("not expected")
	}
	return bf.rows
}
func (bf *mockBlockFile) GetSegmentFile() SegmentFile { return bf.segFile }
func (bf *mockBlockFile) MaxTS() uint64 {
	if bf.appendable {
		panic("not expected")
	}
	return bf.maxTs
}

func (bf *mockBlockFile) WriteData(bat batch.IBatch) error {
	bf.data = bat
	return nil
}

func (bf *mockBlockFile) LoadData() (bat batch.IBatch, err error) {
	bat = bf.data
	return
}

func (sf *mockSegmentFile) IsAppendable() bool { return sf.appendable }
func (sf *mockSegmentFile) IsSorted() bool     { return sf.sorted }
func (sf *mockSegmentFile) GetBlockFile(id uint64) BlockFile {
	bf := sf.files[id]
	if bf == nil {
		bf = mockAppendableBlock(id, nil, sf)
		sf.files[id] = bf
	}
	return bf
}

func (sf *mockSegmentFile) Destory() error {
	for _, bf := range sf.files {
		if err := bf.Destory(); err != nil {
			return err
		}
	}
	return nil
}
