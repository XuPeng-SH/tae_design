package txn

import (
	"bytes"
	"encoding/binary"

	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
)

func MarshalBatch(data batch.IBatch) ([]byte, error) {
	var buf []byte
	if data == nil {
		return buf, nil
	}
	var bbuf bytes.Buffer
	vecs := make([]*gvec.Vector, 0)
	for _, attr := range data.GetAttrs() {
		vec, err := data.GetVectorByAttr(attr)
		if err != nil {
			return buf, err
		}
		v, err := vec.GetLatestView().CopyToVector()
		if err != nil {
			return buf, err
		}
		vecs = append(vecs, v)
	}
	binary.Write(&bbuf, binary.BigEndian, uint16(len(vecs)))
	for _, vec := range vecs {
		vecBuf, _ := vec.Show()
		binary.Write(&bbuf, binary.BigEndian, uint32(len(vecBuf)))
		bbuf.Write(vecBuf)
	}
	return bbuf.Bytes(), nil
}

func UnmarshalBatch(data batch.IBatch, buf []byte) error {
	return nil
}
