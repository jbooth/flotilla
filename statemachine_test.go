package flotilla

import (
	"fmt"
	"log"
	"os"
	"testing"
)

// initialize state machine with default ops
// put some data in it
// confirm commands that return data work
// confirm we can read data
// test command callbacks
// snapshot it out somewhere
// change the data
// load from snapshot
// confirm we still have unchanged data after snapshot
func TestStateMachine(t *testing.T) {
	tempDir := os.TempDir() + "/flotillaStateTest"
	// initialize state machine with default ops
	state, err := newFlotillaState(
		tempDir,
		defaultCommands(),
		"127.0.0.1",
		log.New(os.Stderr, "state machine test", log.LstdFlags),
	)
	if err != nil {
		t.Fatal(err)
	}
	// put some data in it
	result, _ := state.Apply(logForCommand("", 0, "Put", [][]byte{[]byte("defaultDB"), []byte("foo"), []byte("bar")})).(Result)
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	// confirm we can read data
	read, err := state.ReadTxn()
	if err != nil {
		t.Fatal(err)
	}
	db := "defaultDB"
	dbi, err := read.DBIOpen(&db, 0)
	if err != nil {
		t.Fatal(err)
	}
	val, err := read.Get(dbi, []byte("foo"))
	if err != nil {
		t.Fatal(err)
	}
	read.Abort()
	if string(val) != "bar" {
		t.Fatal(fmt.Errorf("Got '%s', expected 'bar' when reading from defaultDB", string(val)))
	}
	// test command callbacks
	cmdcb := state.newCommand()
	// compare and swap that fails
	_ = state.Apply(logForCommand(cmdcb.originAddr, cmdcb.reqNo, "CompareAndSwap", [][]byte{[]byte("defaultDB"), []byte("foo"), []byte("barf"), []byte("bar2")}))
	result = <-cmdcb.result
	if string(result.Response) != "bar" {
		t.Fatal(fmt.Errorf("Got '%s', expected 'bar' when compare/swap from defaultDB", string(val)))
	}
	// compare and swap that succeeds
	_ = state.Apply(logForCommand(cmdcb.originAddr, cmdcb.reqNo, "CompareAndSwap", [][]byte{[]byte("defaultDB"), []byte("foo"), []byte("bar"), []byte("baz")}))
	result = <-cmdcb.result
	if string(result.Response) != "baz" {
		t.Fatal(fmt.Errorf("Got '%s', expected 'bar' when compare/swap from defaultDB", string(val)))
	}
	// confirm we can read baz
	read, err = state.ReadTxn()
	if err != nil {
		t.Fatal(err)
	}
	db = "defaultDB"
	dbi, err = read.DBIOpen(&db, 0)
	if err != nil {
		t.Fatal(err)
	}
	val, err = read.Get(dbi, []byte("foo"))
	if err != nil {
		t.Fatal(err)
	}
	read.Abort()
	if string(val) != "baz" {
		t.Fatal(fmt.Errorf("Got '%s', expected 'bar' when reading from defaultDB", string(val)))
	}
	// snapshot it out somewhere
	snapFilePath := os.TempDir() + "/flotillaStateTest.Snapshot"
	_ = os.Remove(snapFilePath)
	ss, err := state.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapFile, err := os.Create(snapFilePath)
	if err != nil {
		t.Fatal(err)
	}
	err = ss.Persist(&fileSnapshotSink{snapFile})
	if err != nil {
		t.Fatal(err)
	}
	// change the data

	result, _ = state.Apply(logForCommand("", 0, "Put", [][]byte{[]byte("defaultDB"), []byte("foo"), []byte("bar")})).(Result)
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	// confirm we can read data
	read, err = state.ReadTxn()
	if err != nil {
		t.Fatal(err)
	}
	dbi, err = read.DBIOpen(&db, 0)
	if err != nil {
		t.Fatal(err)
	}
	val, err = read.Get(dbi, []byte("foo"))
	if err != nil {
		t.Fatal(err)
	}
	read.Abort()
	if string(val) != "bar" {
		t.Fatal(fmt.Errorf("Got '%s', expected 'bar' when reading from defaultDB", string(val)))
	}
	// load from snapshot
	snapFile, err = os.Open(snapFilePath)
	if err != nil {
		t.Fatal(err)
	}
	err = state.Restore(snapFile)
	if err != nil {
		t.Fatal(err)
	}
	// confirm we still have unchanged data from before snapshot was taken
	read, err = state.ReadTxn()
	if err != nil {
		t.Fatal(err)
	}
	dbi, err = read.DBIOpen(&db, 0)
	if err != nil {
		t.Fatal(err)
	}
	val, err = read.Get(dbi, []byte("foo"))
	if err != nil {
		t.Fatal(err)
	}
	read.Abort()
	if string(val) != "baz" {
		t.Fatal(fmt.Errorf("Got '%s', expected 'bar' when reading from defaultDB", string(val)))
	}
}

//func logForCommand(host string, reqno uint64, cmdName string, args [][]byte) *raft.Log {
//	cmd := &commandReq{}
//	cmd.Args = args
//	cmd.Cmd = cmdName
//	// no callback
//	cmd.OriginAddr = host
//	cmd.Reqno = reqno
//	b, err := encodeMsgPack(cmd)
//	if err != nil {
//		panic(err)
//	}
//	return &raft.Log{
//		Index: 0,
//		Term:  0,
//		Type:  raft.LogCommand,
//		Data:  b.Bytes(),
//	}
//}

type fileSnapshotSink struct {
	*os.File
}

func (f *fileSnapshotSink) Cancel() error {
	return nil
}

func (f *fileSnapshotSink) ID() string {
	return "NOT AN ID"
}
