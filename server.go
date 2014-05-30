package flotilla

import (
	"github.com/jbooth/flotilla/raft"
	"io"
	"log"
	"net"
	"os"
	"time"
)

var (
	dialCodeRaft byte = 0
	dialCodeFlot byte = 1
)

// launches a new DB serving out of dataDir
func NewDB(peers []string, dataDir string, bindAddr string, ops map[string]Command) (DB, error) {
	return nil, nil
}

// Instantiates a new DB serving the ops provided, using the provided dataDir and listener
// If Peers is empty, we start as leader.  Otherwise, connect to the existing leader.
func NewDBXtra(
	peers []string,
	dataDir string,
	listen net.Listener,
	dialer func(string, time.Duration) (net.Conn, error),
	commands map[string]Command,
	logOut io.Writer) (DB, error) {
	lg := log.New(logOut, "flotilla", log.LstdFlags)
	raftDir := dataDir + "/raft"
	mdbDir := dataDir + "/mdb"
	// make sure dirs exist
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	// user supplied commands are prefixed with "user."
	commandsForStateMachine := defaultCommands()
	for cmd, cmdExec := range commands {
		commandsForStateMachine["user."+cmd] = cmdExec
	}
	state, err := newFlotillaState(
		mdbDir,
		commandsForStateMachine,
		listen.Addr().String(),
		lg,
	)
	if err != nil {
		return nil, err
	}
	streamLayers, err := NewMultiStream(listen, dialer, listen.Addr(), lg, dialCodeRaft, dialCodeFlot)
	if err != nil {
		return nil, err
	}
	// if we're the only peer, bootstrap, otherwise, try to join existing
	bootstrap := (len(peers) == 0)
	// start raft server
	raft, err := newRaft(raftDir, streamLayers[dialCodeRaft], state, bootstrap, logOut)
	if err != nil {
		return nil, err
	}

	s := &server{
		raft:      raft,
		state:     state,
		peers:     peers,
		notLeader: make(chan bool, 1),
		rpcLayer:  streamLayers[dialCodeFlot],
	}
	return s, nil
}

type server struct {
	raft       *raft.Raft
	state      *flotillaState
	peers      []string
	rpcLayer   raft.StreamLayer
	leaderLock *sync.Mutex
	leaderConn *connToLeader
	lg         *log.Logger
}

func newRaft(path string, streams raft.StreamLayer, state raft.FSM, logOut io.Writer) (*raft.Raft, error) {
	// Create the MDB store for logs and stable storage, retain up to 8gb
	store, err := raft.NewMDBStoreWithSize(path, 8*1024*1024*1024)
	if err != nil {
		return nil, err
	}

	// Create the snapshot store
	snapshots, err := raft.NewFileSnapshotStore(path, 1, logOut)
	if err != nil {
		store.Close()
		return nil, err
	}

	// Create a transport layer
	trans := raft.NewNetworkTransport(streams, 3, 10*time.Second, logOut)

	// Setup the peer store
	raftPeers := raft.NewJSONPeers(path, trans)

	// Ensure local host is always included
	if bootstrap {
		peers, err := raftPeers.Peers()
		if err != nil {
			store.Close()
			return nil, err
		}
		if !raft.PeerContained(peers, trans.LocalAddr()) {
			return nil, fmt.Errorf("Localhost %s not included in peers %+v", trans.LocalAddr().String(), peers)
		}
	}

	// Setup the Raft server
	raft, err := raft.NewRaft(raft.DefaultConfig(), state, store, store,
		snapshots, raftPeers, trans)
	if err != nil {
		store.Close()
		trans.Close()
		return nil, err
	}
	// wait until we've identified some valid leader
	timeout := time.Now().Add(1 * time.Minute)
	for {
		leader := raft.Leader()
		if leader != nil {
			break
		} else {
			time.Sleep(1 * time.Second)
			if time.Now().After(timeout) {
				return nil, fmt.Errorf("Timed out with no leader elected after 1 minute!")
			}
		}
	}
	return raft, nil
}

// returns addr of leader and whether it is us
func (s *server) Leader() net.Addr {
	return s.raft.Leader()
}

// return if we are leader
func (s *server) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

var commandTimeout = 1 * time.Minute

// public API, executes a command on leader, returns chan which will
// block until command has been replicated to our local replica
func (s *server) Command(cmd string, args [][]byte) <-chan Result {

	if s.IsLeader() {
		cb := s.state.newCommand()
		cmdBytes := bytesForCommand(cb.originAddr, cb.reqNo, cmd, args)
		s.raft.Apply(cmdBytes, commandTimeout)
		return cb.result
	}
	// couldn't exec as leader, fallback to forwarding
	// forward response
	// add to remoteQueue, goroutine will pull initial server response and then either
	// wait on local execution or cancel it
	cb, err := s.dispatchToLeader(cmd, args)
	if err != nil {
		ret := make(chan Result, 1)
		ret <- Result{nil, err}
		return ret
	}
	return cb.result, nil
}

// checks connection state and dispatches the task to
// handlePendingRemote pulls responses from leader
// and then
func (s *server) dispatchToLeader(cmd string, args [][]byte) (*commandCallback, error) {
	s.leaderLock.Lock()
	defer s.leaderLock.Unlock()
	var err error
	for s.Leader() != s.leaderConn.remoteAddr() {
		// reconnect
		s.leaderConn.c.Close()

		return nil, fmt.Errorf("Leader addr %s not equal to our current conn!")

	}
	cb := s.state.newCommand()
	err := s.leaderConn.forwardCommand(cb, cmd, args)
	if err != nil {
		return nil, err
	}
	return cb, nil
}

func (s *server) handlePendingRemote() {

}
