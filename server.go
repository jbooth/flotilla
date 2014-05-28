package flotilla

import (
	"github.com/jbooth/flotilla/mdb"
	"github.com/jbooth/flotilla/raft"
	"log"
	"net"
	"os"
	"time"
)

var (
	dialCodeRaft byte = 0
	dialCodeFlot byte = 1
)

func NewDB(peers []string, dataDir string, bindAddr string, ops map[string]Command) (DB, error) {

}

// Instantiates a new DB serving the ops provided, using the provided dataDir and listener
// If Peers is empty, we start as leader.  Otherwise, connect to the existing leader.
func NewDBXtra(
	peers []string,
	dataDir string,
	listen net.Listener,
	dialer func(string, time.Duration) (net.Conn, error),
	commands map[string]Command,
	lg *log.Logger) (DB, error) {

	raftDir := dataDir + "/" + raft
	mdbDir := dataDir + "/" + mdb
	// make sure dirs exist
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		return nil, err
	}
	if err = os.MkdirAll(dataDir, 0755); err != nil {
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
	raft, err := newRaft(raftDir, streamLayers[dialCodeRaft], state, bootstrap)
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
	raft      raft.Raft
	state     *flotillaState
	peers     []string
	rpcLayer  raft.StreamLayer
	notLeader chan bool
}

func newRaft(path string, streams raft.StreamLayer, state raft.FSM, bootstrap bool) (*raft.Raft, error) {
	// Create the MDB store for logs and stable storage, retain up to 8gb
	store, err := raftmdb.NewMDBStoreWithSize(path, 8*1024*1024*1024)
	if err != nil {
		return err
	}

	// Create the snapshot store
	snapshots, err := raft.NewFileSnapshotStore(path, snapshotsRetained, s.config.LogOutput)
	if err != nil {
		store.Close()
		return err
	}

	// Create a transport layer
	trans := raft.NewNetworkTransport(streams, 3, 10*time.Second, s.config.LogOutput)

	// Setup the peer store
	raftPeers := raft.NewJSONPeers(path, trans)

	// Ensure local host is always included if we are in bootstrap mode
	if bootstrap {
		peers, err := s.raftPeers.Peers()
		if err != nil {
			store.Close()
			return err
		}
		if !raft.PeerContained(peers, trans.LocalAddr()) {
			s.raftPeers.SetPeers(raft.AddUniquePeer(peers, trans.LocalAddr()))
		}
	}

	// Setup the Raft store
	raft, err := raft.NewRaft(raft.DefaultConfig(), state, store, store,
		snapshots, raftPeers, trans)
	if err != nil {
		store.Close()
		trans.Close()
		return nil, err
	}
	return raft, nil
}

// public API, executes a command on leader, returns chan which will
// block until command has been replicated to our local replica
func (s *server) Command(cmd string, args [][]byte) <-chan Result {
	if s.raft.State() == raft.Leader {
		// get clientCallback, future, and wait till executed locally
		value, err := s.execLocal(command)
		if err != nil {
			log.Error("Cannot run command %#v. %s", command, err)
		}
		return value, err
	}
	// couldn't exec as leader, fallback to forwarding
	// forward response
	// add to remoteQueue, goroutine will pull initial server response and then either
	// wait on local execution or cancel it
	if leader, ok := s.leaderConnectString(); !ok {
		return nil, errors.New("Couldn't connect to the cluster leader...")
	} else {
		return SendCommandToServer(leader, command)
	}

	return nil, nil
}

func (s *Server) join(peerAddr string) {

}
