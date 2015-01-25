package flotilla

import (
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/jbooth/gomdb"
	"io"
	"log"
	"os"
	"sync"
	"syscall"
)

// finite state machine to interop with raft
// provides methods for:
//   applying commands to local state
//   snapshotting
//   waiting until a given logID has been processed locally
type flotillaState struct {
	env            *env
	commands       map[string]Command
	addr           string // used for uniqueness
	reqnoCtr       uint64
	dataPath       string
	tempPath       string
	localCallbacks map[uint64]*commandCallback
	l              *sync.Mutex // guards callbacks and reqnoCtr
	lg             *log.Logger
}

func newFlotillaState(dbPath string, commands map[string]Command, addr string, lg *log.Logger) (*flotillaState, error) {
	lg.Printf("New flotilla state at path %s, listening on %s\n", dbPath, addr)
	// current data stored here
	dataPath := dbPath + "/data"
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		return nil, err
	}
	// temp dir for snapshots
	tempPath := dbPath + "/temp"
	if err := os.MkdirAll(tempPath, 0755); err != nil {
		return nil, err
	}
	env, err := newenv(dataPath)
	if err != nil {
		return nil, err
	}
	return &flotillaState{
		env,
		commands,
		addr,
		0,
		dataPath,
		tempPath,
		make(map[uint64]*commandCallback),
		new(sync.Mutex),
		lg,
	}, nil
}

// Apply log is invoked once a log entry is commited
// always returns type Result (no pointer)
func (f *flotillaState) Apply(l *raft.Log) interface{} {
	cmd := &commandReq{}
	err := decodeMsgPack(l.Data, cmd)
	if err != nil {
		return Result{nil, err}
	}
	// open write txn
	txn, err := f.env.writeTxn()
	if err != nil {
		return &Result{nil, err}
	}
	f.lg.Printf("Executing command name %s", cmd.Cmd)
	// execute command, get results
	cmdExec, ok := f.commands[cmd.Cmd]
	result := Result{nil, nil}
	if !ok {
		f.lg.Printf("Received invalid command %s", cmd.Cmd)
		result.Err = fmt.Errorf("No command registered with name %s", cmd.Cmd)
	} else {
		result.Response, result.Err = cmdExec(cmd.Args, txn)
	}
	// confirm txn handle closed (our txn wrapper keeps track of state so we don't abort committed txn)
	txn.Abort()
	// check for callback
	f.lg.Printf("Finished command %s with result %s err %s", cmd.Cmd, string(result.Response), result.Err)
	f.l.Lock()
	defer f.l.Unlock()
	cb, ok := f.localCallbacks[cmd.Reqno]
	if ok {
		cb.result <- result
	}
	return result
}

func (s *flotillaState) ReadTxn() (*mdb.Txn, error) {
	// lock to make sure we don't race with Restore()
	s.l.Lock()
	defer s.l.Unlock()
	return s.env.readTxn()
}

type commandCallback struct {
	originAddr string
	reqNo      uint64
	f          *flotillaState
	result     chan Result
}

func (c *commandCallback) cancel() {
	c.f.l.Lock()
	defer c.f.l.Unlock()
	delete(c.f.localCallbacks, c.reqNo)
}

func (f *flotillaState) newCommand() *commandCallback {
	f.l.Lock()
	defer f.l.Unlock()
	// bump reqno
	f.reqnoCtr++
	ret := &commandCallback{
		f.addr,
		f.reqnoCtr,
		f,
		make(chan Result, 1),
	}
	// insert into our map
	f.localCallbacks[ret.reqNo] = ret
	// return
	return ret
}

// Snapshot is used to support log compaction. This call should
// return an FSMSnapshot which can be used to save a point-in-time
// snapshot of the FSM. Apply and Snapshot are not called in multiple
// threads, but Apply will be called concurrently with Persist. This means
// the FSM should be implemented in a fashion that allows for concurrent
// updates while a snapshot is happening.
func (f *flotillaState) Snapshot() (raft.FSMSnapshot, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	ret := &flotillaSnapshot{r, w, f.env.e, make(chan error, 1)}
	// start snapshot to guarantee it's a snapshot of state as this call is made
	go ret.pipeCopy()
	return ret, nil
}

type flotillaSnapshot struct {
	pipeR   *os.File
	pipeW   *os.File
	env     *mdb.Env
	copyErr chan error
}

// starts streaming snapshot into one end of pipe
func (s *flotillaSnapshot) pipeCopy() {
	defer s.pipeW.Close()
	s.copyErr <- s.env.CopyFd(int(s.pipeW.Fd())) // buffered chan here
}

// Persist should dump all necessary state to the WriteCloser,
// and invoke close when finished or call Cancel on error.
func (s *flotillaSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()
	defer s.pipeR.Close()
	_, e1 := io.Copy(sink, s.pipeR)
	e2 := <-s.copyErr

	if e2 != nil {
		sink.Cancel()
		return fmt.Errorf("Error copying snapshot to pipe: %s", e2)
	}
	if e1 != nil {
		sink.Cancel()
	} else {
		sink.Close()
	}
	return e1
}

// Release is invoked when we are finished with the snapshot
func (s *flotillaSnapshot) Release() {
	// no-op, we close all handles in the Persist and pipeCopy methods
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
// Note, this command is called concurrently with open read txns, so we handle that
func (f *flotillaState) Restore(in io.ReadCloser) error {
	// stream to filePath.tmp
	tempData := f.tempPath + "/data.mdb"
	_ = os.Remove(tempData)
	tempFile, err := os.Create(tempData)
	if err != nil {
		return err
	}
	defer tempFile.Close()
	if _, err = io.Copy(tempFile, in); err != nil {
		return err
	}
	// unlink existing DB and move new one into place
	// can't atomically rename directories so have to lock for this
	f.l.Lock()
	defer f.l.Unlock()
	if err = syscall.Unlink(f.dataPath + "/data.mdb"); err != nil {
		return err
	}
	if err = syscall.Unlink(f.dataPath + "/lock.mdb"); err != nil {
		return err
	}
	if err = os.Rename(tempData, f.dataPath+"/data.mdb"); err != nil {
		return err
	}
	// mark existing env as closeable when all outstanding txns finish
	// posix holds onto our data until we release FD
	f.env.Close()
	// re-initialize env
	f.env, err = newenv(f.dataPath)
	return err
}
