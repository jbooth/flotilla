package flotilla

import (
	"github.com/jbooth/flotilla/gomdb"
	"github.com/jbooth/flotilla/raft"
)

// finite state machine to interop with raft
// provides methods for:
//   applying commands to local state
//   snapshotting
//   waiting until a given logID has been processed locally
type FlotillaState struct {
	commands map[string]Command
	addr     string // used for uniqueness
	reqnoCtr uint64
	dbPath   string
	env      *mdb.Env
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
	ret := &FlotillaSnapshot{r, w, f.env, make(chan error, 1)}
	// start snapshot to guarantee it's a snapshot of state as this call is made
	go ret.pipeCopy()
	return ret, nil
}

// Apply log is invoked once a log entry is commited
func (f *FlotillaState) Apply(l *raft.Log) interface{} {
	return nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (f *FlotillaState) Restore(io.ReadCloser) error {
	// stream to filePath.tmp

	// move over existing DB

	// re-initialize MDB
	return nil
}

type commandCallback struct {
	originAddr string
	reqNo      uint64
}

func (f *FlotillaState) newCommand() {

}

type FlotillaSnapshot struct {
	pipeR   *os.File
	pipeW   *os.File
	env     *mdb.Env
	copyErr chan error
}

func (s *FlotillaSnapshot) pipeCopy() {
	defer s.pipeW.Close()
	s.copyErr <- env.CopyFd(pipeW.Fd()) // buffered chan here
}

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
}
