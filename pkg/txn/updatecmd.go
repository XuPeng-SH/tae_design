package txn

import (
	"bytes"
	"encoding/binary"
	"io"
)

func init() {
	RegisterCmdFactory(CmdUpdate, func() TxnCmd {
		return NewEmptyUpdateCmd()
	})
}

type UpdateCmd struct {
	*BaseCustomizedCmd
	updates *blockUpdates
}

func NewEmptyUpdateCmd() *UpdateCmd {
	updates := NewBlockUpdates(nil, nil, nil, nil)
	return NewUpdateCmd(0, updates)
}

func NewUpdateCmd(id uint32, updates *blockUpdates) *UpdateCmd {
	impl := &UpdateCmd{
		updates: updates,
	}
	impl.BaseCustomizedCmd = NewBaseCustomizedCmd(id, impl)
	return impl
}

// TODO
func (c *UpdateCmd) String() string {
	return ""
}

func (c *UpdateCmd) GetType() int16 { return CmdUpdate }

func (c *UpdateCmd) WriteTo(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, c.GetType()); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, c.ID); err != nil {
		return
	}
	err = c.updates.WriteTo(w)
	return
}

func (c *UpdateCmd) ReadFrom(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &c.ID); err != nil {
		return
	}
	err = c.updates.ReadFrom(r)
	return
}

func (c *UpdateCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if err = c.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (c *UpdateCmd) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	return c.ReadFrom(bbuf)
}
