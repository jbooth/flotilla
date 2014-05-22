package flotilla

import (
	"fmt"
	"github.com/jbooth/flotilla/raft"
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
	result, _ := state.Apply(logForCommand("Put", [][]byte{[]byte("defaultDB"), []byte("foo"), []byte("bar")})).(Result)
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
	if string(val) != "bar" {
		t.Fatal(fmt.Errorf("Got '%s', expected 'bar' when reading from defaultDB", string(val)))
	}
	// test command callbacks

	// snapshot it out somewhere

	// change the data

	// load from snapshot

	// confirm we still have unchanged data after snapshot

}

func logForCommand(cmdName string, args [][]byte) *raft.Log {
	cmd := &commandReq{}
	cmd.Args = args
	cmd.Cmd = cmdName
	// no callback
	cmd.OriginAddr = ""
	cmd.Reqno = 0
	b, err := encodeMsgPack(cmd)
	if err != nil {
		panic(err)
	}
	return &raft.Log{
		Index: 0,
		Term:  0,
		Type:  raft.LogCommand,
		Data:  b.Bytes(),
	}
}
