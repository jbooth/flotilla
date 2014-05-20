package flotilla

import (
	"github.com/jbooth/flotilla/gomdb"
	"github.com/jbooth/flotilla/raft"
	"io"
	"log"
	"sync"
	"syscall"
)

// finite state machine to interop with raft
// provides methods for:
//   applying commands to local state
//   snapshotting
//   waiting until a given logID has been processed locally
type FlotillaState struct {
	env            *env
	commands       map[string]Command
	addr           string // used for uniqueness
	reqnoCtr       uint64
	dbPath         string
	localCallbacks map[uint64]*commandCallback
	reqnoCtr       uint64
	l              *sync.Mutex // guards callbacks and reqnoCtr
	lg             *log.Logger
}

func NewFlotillaState(dbPath string, commands map[string]Command) {

}

// Apply log is invoked once a log entry is commited
func (f *FlotillaState) Apply(l *raft.Log) interface{} {
	cmd := &commandReq{}
	err := decodeMsgPack(l.Data, cmd)
	if err != nil {
		return Result{[]byte{}, err}
	}
	// open write txn
	// execute command, get results
	// check for callback
	return nil
}

type commandCallback struct {
	originAddr string
	reqNo      uint64
	f          *FlotillaState
	result     chan Result
}

func (c *commandCallback) cancel() {
	//
}

func (f *FlotillaState) newCommand() *commandCallback {
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
func (f *FlotillaState) Snapshot() (raft.FSMSnapshot, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	ret := &FlotillaSnapshot{r, w, f.env.e, make(chan error, 1)}
	// start snapshot to guarantee it's a snapshot of state as this call is made
	go ret.pipeCopy()
	return ret, nil
}

type FlotillaSnapshot struct {
	pipeR   *os.File
	pipeW   *os.File
	env     *mdb.Env
	copyErr chan error
}

// starts streaming snapshot into one end of pipe
func (s *FlotillaSnapshot) pipeCopy() {
	defer s.pipeW.Close()
	s.copyErr <- env.CopyFd(pipeW.Fd()) // buffered chan here
}

// pulls
// Persist should dump all necessary state to the WriteCloser,
// and invoke close when finished or call Cancel on error.
func (s *FlotillaSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()
	defer pipeR.Close()
	e1 := io.Copy(pipeR, sink)
	e2 := <-s.copyErr
	if e2 != nil {
		return fmt.Sprintf("Error copying snapshot to pipe: %s", e2)
	}
	return e1
}

// Release is invoked when we are finished with the snapshot
func (s *FlotillaSnapshot) Release() {
	// no-op, we close all handles in Persist and pipeCopy methods
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
// Note, this command is called concurrently with open read txns, so we handle that
func (f *FlotillaState) Restore(in io.ReadCloser) error {
	// stream to filePath.tmp
	tempPath = f.dbPath + ".tmp"
	f, err := os.OpenFile(tempPath, os.WR_ONLY, 0755)
	if err != nil {
		return err
	}
	defer f.Close()
	err = io.Copy(f, in)
	if err = io.Copy(f, in); err != nil {
		return err
	}
	f.l.Lock()
	defer f.l.Unlock()
	// unlink existing DB and move new one into place
	if err = syscall.Unlink(f.dbPath); err != nil {
		return err
	}
	if err = os.Rename(tempPath, f.dbPath); err != nil {
		return err
	}
	// mark existing env as closeable when all outstanding txns finish
	f.env.Close()
	// re-initialize env
	f.env, err = newenv(f.dbPath)
	return err
}
