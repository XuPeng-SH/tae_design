package common

import (
	"encoding/binary"
	"io"
)

func WriteString(str string, w io.Writer) (n int64, err error) {
	buf := []byte(str)
	if err = binary.Write(w, binary.BigEndian, uint16(len(buf))); err != nil {
		return
	}
	wn, err := w.Write(buf)
	return int64(wn + 2), err
}
