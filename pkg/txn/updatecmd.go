package txn

import (
	"encoding/binary"
	"io"
)

func init() {
	RegisterCmdFactory(CmdUpdate, func() TxnCmd {
		// return NewE
		return nil
	})
}

type UpdateCmd struct {
	*BaseCustomizedCmd
	updates *blockUpdates
}

func NewEmptyUpdateCmd() *UpdateCmd {
	return NewUpdateCmd(0, nil)
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
	if err = binary.Write(w, binary.BigEndian, c.ID); err != nil {
		return
	}
	// TODO
	return
}

func (c *UpdateCmd) ReadFrom(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &c.ID); err != nil {
		return
	}
	// TODO
	return
}

func (c *UpdateCmd) Marshal() (buf []byte, err error) {
	// TODO
	return
}

func (c *UpdateCmd) Unmarshal(buf []byte) error {
	// TODO
	return nil
}
