package txnimpl

import (
	"tae/pkg/iface/txnif"
	"tae/pkg/txn/txnbase"

	"github.com/jiangxinmeng1/logstore/pkg/entry"
	"github.com/sirupsen/logrus"
)

type commandManager struct {
	cmd    *txnbase.ComposedCmd
	csn    int
	driver txnbase.NodeDriver
}

func newCommandManager(driver txnbase.NodeDriver) *commandManager {
	return &commandManager{
		cmd:    txnbase.NewComposedCmd(),
		driver: driver,
	}
}

func (mgr *commandManager) GetCSN() int {
	return mgr.csn
}

func (mgr *commandManager) AddCmd(cmd txnif.TxnCmd) {
	mgr.cmd.AddCmd(cmd)
	mgr.csn++
}

func (mgr *commandManager) ApplyTxnRecord() (logEntry entry.Entry, err error) {
	if mgr.driver == nil {
		return
	}
	var buf []byte
	if buf, err = mgr.cmd.Marshal(); err != nil {
		panic(err)
	}
	logEntry = entry.GetBase()
	logEntry.SetType(ETTxnRecord)
	logEntry.Unmarshal(buf)

	lsn, err := mgr.driver.AppendEntry(txnbase.GroupC, logEntry)
	logrus.Infof("ApplyTxnRecord LSN=%d", lsn)
	return
}
