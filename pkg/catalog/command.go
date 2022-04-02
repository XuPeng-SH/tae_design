package catalog

import (
	"io"
	"tae/pkg/iface/txnif"
	"tae/pkg/txn/txnbase"
)

const (
	CmdCreateDatabase = int16(256) + iota
	CmdDropDatabase
	CmdCreateTable
	CmdDropTable
	// CmdCreateSegment
	// CmdDropSegment
)

func init() {
	txnif.RegisterCmdFactory(CmdCreateDatabase, func() txnif.TxnCmd {
		return newEmptyDBCmd()
	})
}

type dbCmd struct {
	*txnbase.BaseCustomizedCmd
	entry *DBEntry
}

func newEmptyDBCmd() *dbCmd {
	return newCreateDBCmd(0, nil)
}

func newCreateDBCmd(id uint32, entry *DBEntry) *dbCmd {
	impl := &dbCmd{
		entry: entry,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

// TODO
func (cmd *dbCmd) String() string {
	return ""
}
func (cmd *dbCmd) GetType() int16 { return CmdCreateDatabase }

func (cmd *dbCmd) WriteTo(w io.Writer) (err error) {
	return
}
func (cmd *dbCmd) Marshal() (buf []byte, err error) {
	return
}
func (cmd *dbCmd) ReadFrom(r io.Reader) (err error) {
	return
}
func (cmd *dbCmd) Unmarshal(buf []byte) (err error) {
	return
}
