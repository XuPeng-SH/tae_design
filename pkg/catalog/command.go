package catalog

import (
	"bytes"
	"encoding/binary"
	"io"
	"tae/pkg/common"
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
	txnif.RegisterCmdFactory(CmdCreateDatabase, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
	txnif.RegisterCmdFactory(CmdDropDatabase, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
	txnif.RegisterCmdFactory(CmdCreateTable, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
	txnif.RegisterCmdFactory(CmdDropTable, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
}

type entryCmd struct {
	*txnbase.BaseCustomizedCmd
	db      *DBEntry
	table   *TableEntry
	entry   *BaseEntry
	cmdType int16
}

func newEmptyEntryCmd(cmdType int16) *entryCmd {
	return newDBCmd(0, cmdType, nil)
}

func newTableCmd(id uint32, cmdType int16, entry *TableEntry) *entryCmd {
	impl := &entryCmd{
		table:   entry,
		cmdType: cmdType,
		entry:   entry.BaseEntry,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newDBCmd(id uint32, cmdType int16, entry *DBEntry) *entryCmd {
	impl := &entryCmd{
		db:      entry,
		cmdType: cmdType,
	}
	if entry != nil {
		impl.entry = entry.BaseEntry
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

// TODO
func (cmd *entryCmd) String() string {
	return ""
}
func (cmd *entryCmd) GetType() int16 { return cmd.cmdType }

func (cmd *entryCmd) WriteTo(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, cmd.GetType()); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, cmd.ID); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, cmd.entry.GetID()); err != nil {
		return
	}
	switch cmd.GetType() {
	case CmdCreateDatabase:
		if err = binary.Write(w, binary.BigEndian, cmd.entry.CreateAt); err != nil {
			return
		}
		if _, err = common.WriteString(cmd.db.name, w); err != nil {
			return
		}
	case CmdCreateTable:
		if err = binary.Write(w, binary.BigEndian, cmd.table.db.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.CreateAt); err != nil {
			return
		}
		var schemaBuf []byte
		if schemaBuf, err = cmd.table.schema.Marshal(); err != nil {
			return
		}
		if _, err = w.Write(schemaBuf); err != nil {
			return
		}
	case CmdDropTable:
		if err = binary.Write(w, binary.BigEndian, cmd.table.db.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.DeleteAt); err != nil {
			return
		}
	case CmdDropDatabase:
		if err = binary.Write(w, binary.BigEndian, cmd.entry.DeleteAt); err != nil {
			return
		}
	}
	return
}
func (cmd *entryCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if err = cmd.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (cmd *entryCmd) ReadFrom(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &cmd.ID); err != nil {
		return
	}
	cmd.entry = &BaseEntry{}
	if err = binary.Read(r, binary.BigEndian, &cmd.entry.ID); err != nil {
		return
	}
	switch cmd.GetType() {
	case CmdCreateDatabase:
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.CreateAt); err != nil {
			return
		}
		cmd.db = &DBEntry{
			BaseEntry: cmd.entry,
		}
		if cmd.db.name, err = common.ReadString(r); err != nil {
			return
		}
	case CmdCreateTable:
		cmd.db = &DBEntry{BaseEntry: &BaseEntry{}}
		if err = binary.Read(r, binary.BigEndian, &cmd.db.ID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.CreateAt); err != nil {
			return
		}
		cmd.table = &TableEntry{
			BaseEntry: cmd.entry,
			schema:    new(Schema),
		}
		if err = cmd.table.schema.ReadFrom(r); err != nil {
			return
		}
	case CmdDropTable:
		cmd.db = &DBEntry{BaseEntry: &BaseEntry{}}
		if err = binary.Read(r, binary.BigEndian, &cmd.db.ID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.DeleteAt); err != nil {
			return
		}
	case CmdDropDatabase:
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.DeleteAt); err != nil {
			return
		}
	}
	return
}
func (cmd *entryCmd) Unmarshal(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	err = cmd.ReadFrom(bbuf)
	return
}
