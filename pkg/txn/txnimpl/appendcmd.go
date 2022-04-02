package txnimpl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"tae/pkg/iface/txnif"
	"tae/pkg/txn/txnbase"
)

const (
	CmdAppend int16 = txnbase.CmdCustomized + iota
	CmdUpdate
	CmdDelete
)

func init() {
	txnif.RegisterCmdFactory(CmdAppend, func() txnif.TxnCmd {
		return NewEmptyAppendCmd()
	})
}

type AppendCmd struct {
	*txnbase.BaseCustomizedCmd
	txnbase.ComposedCmd
	Node InsertNode
}

func NewEmptyAppendCmd() *AppendCmd {
	return NewAppendCmd(0, nil)
}

func NewAppendCmd(id uint32, node InsertNode) *AppendCmd {
	impl := &AppendCmd{
		ComposedCmd: *txnbase.NewComposedCmd(),
		Node:        node,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func (c *AppendCmd) String() string {
	s := fmt.Sprintf("AppendCmd: ID=%d", c.ID)
	s = fmt.Sprintf("%s\n%s", s, c.ComposedCmd.ToString("\t"))
	s = fmt.Sprintf("%s\n%s",s,c.Node.SubCommandInfoToString())
	return s
}

func (e *AppendCmd) GetType() int16 { return CmdAppend }
func (c *AppendCmd) WriteTo(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, c.ID); err != nil {
		return
	}
	if err = c.ComposedCmd.WriteTo(w); err != nil {
		return
	}
	err = c.Node.WriteSubCommandInfo(w)
	return err
}

func (c *AppendCmd) ReadFrom(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &c.ID); err != nil {
		return
	}
	if err = c.ComposedCmd.ReadFrom(r); err != nil {
		return
	}
	err = c.Node.ReadSubCommandInfo(r)
	return
}

func (c *AppendCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if err = c.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (c *AppendCmd) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	err := c.ReadFrom(bbuf)
	return err
}
