package txn

import (
	"bytes"
	"encoding/binary"
	"io"

	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"github.com/sirupsen/logrus"
)

func MarshalBatch(types []types.Type, data batch.IBatch) ([]byte, error) {
	var buf []byte
	if data == nil {
		return buf, nil
	}
	var bbuf bytes.Buffer
	vecs := make([]vector.IVectorNode, 0)
	for _, attr := range data.GetAttrs() {
		vec, err := data.GetVectorByAttr(attr)
		if err != nil {
			return buf, err
		}
		v := vec.(vector.IVectorNode)
		vecs = append(vecs, v)
	}
	binary.Write(&bbuf, binary.BigEndian, uint32(0))
	binary.Write(&bbuf, binary.BigEndian, uint16(len(vecs)))
	binary.Write(&bbuf, binary.BigEndian, uint32(data.Length()))
	bufs := make([][]byte, len(vecs))
	for i, vec := range vecs {
		vecBuf, _ := vec.Marshal()
		bufs[i] = vecBuf
		typeBuf := encoding.EncodeType(types[i])
		_, err := bbuf.Write(typeBuf)
		if err != nil {
			return nil, err
		}
		binary.Write(&bbuf, binary.BigEndian, uint32(len(vecBuf)))
	}
	for _, colBuf := range bufs {
		bbuf.Write(colBuf)
	}
	buf = bbuf.Bytes()
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(buf)))
	return buf, nil
}

func UnmarshalBatch(buf []byte) (vecTypes []types.Type, bat batch.IBatch, err error) {
	r := bytes.NewBuffer(buf)
	return UnmarshalBatchFrom(r)
}

func UnmarshalBatchFrom(r io.Reader) (vecTypes []types.Type, bat batch.IBatch, err error) {
	var size uint32
	var vecs uint16
	pos := 0
	if binary.Read(r, binary.BigEndian, &size); err != nil {
		return
	}
	buf := make([]byte, size-4)
	if _, err = r.Read(buf); err != nil {
		return
	}
	bbuf := bytes.NewBuffer(buf)
	if binary.Read(bbuf, binary.BigEndian, &vecs); err != nil {
		return
	}
	var rows uint32
	if binary.Read(bbuf, binary.BigEndian, &rows); err != nil {
		return
	}
	pos += 2 + 4
	logrus.Info(pos)
	lens := make([]uint32, vecs)
	vecTypes = make([]types.Type, vecs)
	for i := uint16(0); i < vecs; i++ {
		colType := encoding.DecodeType(buf[pos : pos+encoding.TypeSize])
		vecTypes[i] = colType
		pos += encoding.TypeSize
		lens[i] = binary.BigEndian.Uint32(buf[pos:])
		pos += 4
	}

	attrs := make([]int, vecs)
	cols := make([]vector.IVector, vecs)
	for i := 0; i < int(vecs); i++ {
		col := vector.NewVector(vecTypes[i], uint64(rows))
		cols[i] = col
		attrs[i] = i
		if col.(vector.IVectorNode).Unmarshal(buf[pos : pos+int(lens[i])]); err != nil {
			return
		}
		pos += int(lens[i])
	}

	bat, err = batch.NewBatch(attrs, cols)
	return
}

func GetValue(col *gvec.Vector, row uint32) interface{} {
	vals := col.Col
	switch col.Typ.Oid {
	case types.T_int8:
		data := vals.([]int8)
		return data[row]
	case types.T_int16:
		data := vals.([]int16)
		return data[row]
	case types.T_int32:
		data := vals.([]int32)
		return data[row]
	case types.T_int64:
		data := vals.([]int64)
		return data[row]
	case types.T_uint8:
		data := vals.([]uint8)
		return data[row]
	case types.T_uint16:
		data := vals.([]uint16)
		return data[row]
	case types.T_uint32:
		data := vals.([]uint32)
		return data[row]
	case types.T_uint64:
		data := vals.([]uint64)
		return data[row]
	case types.T_decimal:
		data := vals.([]types.Decimal)
		return data[row]
	case types.T_float32:
		data := vals.([]float32)
		return data[row]
	case types.T_float64:
		data := vals.([]float64)
		return data[row]
	case types.T_date:
		data := vals.([]types.Date)
		return data[row]
	case types.T_datetime:
		data := vals.([]types.Datetime)
		return data[row]
	case types.T_char, types.T_varchar, types.T_json:
		data := vals.(*types.Bytes)
		s := data.Offsets[row]
		e := data.Lengths[row]
		return string(data.Data[s:e])
	default:
		return vector.VecTypeNotSupportErr
	}
	return nil
}

func SplitBatch(bat *gbat.Batch, cnt int) []*gbat.Batch {
	if cnt == 1 {
		return []*gbat.Batch{bat}
	}
	length := gvec.Length(bat.Vecs[0])
	rows := length / cnt
	if length%cnt == 0 {
		bats := make([]*gbat.Batch, 0, cnt)
		for i := 0; i < cnt; i++ {
			newBat := gbat.New(true, bat.Attrs)
			for j := 0; j < len(bat.Vecs); j++ {
				window := gvec.New(bat.Vecs[j].Typ)
				gvec.Window(bat.Vecs[j], i*rows, (i+1)*rows, window)
				newBat.Vecs[j] = window
			}
			bats = append(bats, newBat)
		}
		return bats
	}
	rowArray := make([]int, 0)
	if length/cnt == 0 {
		for i := 0; i < length; i++ {
			rowArray = append(rowArray, 1)
		}
	} else {
		left := length
		for i := 0; i < cnt; i++ {
			if left >= rows {
				rowArray = append(rowArray, rows)
			} else {
				rowArray = append(rowArray, left)
			}
			left -= rows
		}
	}
	start := 0
	bats := make([]*gbat.Batch, 0, cnt)
	for _, row := range rowArray {
		newBat := gbat.New(true, bat.Attrs)
		for j := 0; j < len(bat.Vecs); j++ {
			window := gvec.New(bat.Vecs[j].Typ)
			gvec.Window(bat.Vecs[j], start, start+row, window)
			newBat.Vecs[j] = window
		}
		start += row
		bats = append(bats, newBat)
	}
	return bats
}

func EstimateSize(bat *gbat.Batch, offset, length uint32) uint64 {
	size := uint64(0)
	for _, vec := range bat.Vecs {
		colSize := length * uint32(vec.Typ.Size)
		size += uint64(colSize)
	}
	return size
}

func CopyToIBatch(data *gbat.Batch) (bat batch.IBatch, err error) {
	vecs := make([]vector.IVector, len(data.Vecs))
	attrs := make([]int, len(data.Vecs))
	for i, vec := range data.Vecs {
		attrs[i] = i
		vecs[i] = vector.NewVector(vec.Typ, uint64(MaxNodeRows))
		_, err = vecs[i].AppendVector(vec, 0)
		if err != nil {
			return
		}
	}
	bat, err = batch.NewBatch(attrs, vecs)
	return
}
