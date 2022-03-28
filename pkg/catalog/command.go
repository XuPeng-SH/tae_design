package catalog

import (
	"io"
	"tae/pkg/txn/txnbase"
)

const (
	CmdCreateDatabase = int16(256) + iota
	// CmdDropDatabase
	// CmdCreateTable
	// CmdDropTable
	// CmdCreateSegment
	// CmdDropSegment
)

func init() {
	txnbase.RegisterCmdFactory(CmdCreateDatabase, func() txnbase.TxnCmd {
		return newEmptyCreateDBCmd()
	})
}

type createDBCmd struct {
	*txnbase.BaseCustomizedCmd
	entry *DBEntry
}

func newEmptyCreateDBCmd() *createDBCmd {
	return newCreateDBCmd(0, nil)
}

func newCreateDBCmd(id uint32, entry *DBEntry) *createDBCmd {
	impl := &createDBCmd{
		entry: entry,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

// TODO
func (cmd *createDBCmd) String() string {
	return ""
}
func (cmd *createDBCmd) GetType() int16 { return CmdCreateDatabase }

func (cmd *createDBCmd) WriteTo(w io.Writer) (err error) {
	return
}
func (cmd *createDBCmd) Marshal() (buf []byte, err error) {
	return
}
func (cmd *createDBCmd) ReadFrom(r io.Reader) (err error) {
	return
}
func (cmd *createDBCmd) Unmarshal(buf []byte) (err error) {
	return
}
