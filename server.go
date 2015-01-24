package flotilla

import (
	"fmt"
	"github.com/hashicorp/raft"
	mdb "github.com/jbooth/gomdb"
	raftmdb "github.com/jbooth/raft-mdb"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

var (
	dialCodeRaft byte = 0
	dialCodeFlot byte = 1
)

// launches a new DB serving out of dataDir
func NewDefaultDB(peers []string, dataDir string, bindAddr string, ops map[string]Command) (DefaultOpsDB, error) {
	laddr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	listen, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		return nil, err
	}
	db, err := NewDB(
		peers,
		dataDir,
		listen,
		defaultDialer,
		ops,
		os.Stderr,
	)
	if err != nil {
		return nil, err
	}

	// wrap with standard ops
	return dbOps{db}, nil
}

// Instantiates a new DB serving the ops provided, using the provided dataDir and listener
// If Peers is empty, we start as the sole leader.  Otherwise, connect to the existing leader.
func NewDB(
	peers []string,
	dataDir string,
	listen net.Listener,
	dialer func(string, time.Duration) (net.Conn, error),
	commands map[string]Command,
	logOut io.Writer) (DB, error) {
	lg := log.New(logOut, "flotilla", log.LstdFlags)
	lg.Printf("Starting server with peers %+v, dataDir %s\n", peers, dataDir)
	raftDir := dataDir + "/raft"
	mdbDir := dataDir + "/mdb"
	// make sure dirs exist
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	commandsForStateMachine := defaultCommands()
	for cmd, cmdExec := range commands {
		_, ok := commandsForStateMachine[cmd]
		if ok {
			lg.Printf("WARNING overriding command %s with user-defined command", cmd)
		}
		commandsForStateMachine[cmd] = cmdExec
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
	// start raft server
	raft, err := newRaft(peers, raftDir, streamLayers[dialCodeRaft], state, logOut)
	if err != nil {
		return nil, err
	}
	s := &server{
		raft:       raft,
		state:      state,
		peers:      peers,
		rpcLayer:   streamLayers[dialCodeFlot],
		leaderLock: new(sync.Mutex),
		leaderConn: nil,
		lg:         log.New(logOut, "flotilla", log.LstdFlags),
	}
	// serve followers
	go s.serveFollowers()
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

func newRaft(peers []string, path string, streams raft.StreamLayer, state raft.FSM, logOut io.Writer) (*raft.Raft, error) {
	// Create the MDB store for logs and stable storage, retain up to 8gb
	store, err := raftmdb.NewMDBStoreWithSize(path, 8*1024*1024*1024)
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
	fmt.Fprintf(logOut, "server.newRaft Resolving peers %+v", peers)
	peerAddrs := make([]net.Addr, len(peers), len(peers))
	for idx, p := range peers {
		peerAddrs[idx], err = net.ResolveTCPAddr("tcp", p)
		if err != nil {
			return nil, err
		}
	}
	fmt.Fprintf(logOut, "server.newRaft Setting peer addrs %+v", peerAddrs)
	raftPeers := raft.NewJSONPeers(path, trans)
	if err = raftPeers.SetPeers(peerAddrs); err != nil {
		return nil, err
	}
	// Ensure local host is always included
	peerAddrs, err = raftPeers.Peers()
	if err != nil {
		store.Close()
		return nil, err
	}
	if !raft.PeerContained(peerAddrs, trans.LocalAddr()) {
		return nil, fmt.Errorf("Localhost %s not included in peers %+v", trans.LocalAddr().String(), peers)
	}

	// Setup the Raft server
	raftCfg := raft.DefaultConfig()
	if len(peers) == 1 {
		raftCfg.EnableSingleNode = true
	}
	raft, err := raft.NewRaft(raftCfg, state, store, store,
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
		fmt.Fprintf(logOut, "server.newRaft Identified leader %s from host %s\n", leader, trans.LocalAddr().String())
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

func (s *server) serveFollowers() {
	for {
		conn, err := s.rpcLayer.Accept()
		if err != nil {
			s.lg.Printf("ERROR accepting from %s : %s", s.rpcLayer.Addr().String(), err)
			return
		}
		go serveFollower(s.lg, conn, s)
	}
}

// returns addr of leader
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
	cb, err := s.dispatchToLeader(cmd, args)
	if err != nil {
		if cb != nil {
			cb.cancel()
		}
		ret := make(chan Result, 1)
		ret <- Result{nil, err}
		return ret
	}
	return cb.result
}

// checks connection state and dispatches the task to leader
// returns a callback registered with our state machine
func (s *server) dispatchToLeader(cmd string, args [][]byte) (*commandCallback, error) {
	s.leaderLock.Lock()
	defer s.leaderLock.Unlock()
	var err error
	if s.leaderConn == nil || s.Leader() != s.leaderConn.remoteAddr() {
		// reconnect
		if s.leaderConn != nil {
			s.leaderConn.c.Close()
		}
		newConn, err := s.rpcLayer.Dial(s.Leader().String(), 1*time.Minute)
		if err != nil {
			return nil, fmt.Errorf("Couldn't connect to leader at %s", s.Leader().String())
		}
		s.lg.Printf("Connecting to leader %s from follower %s\n", s.Leader().String(), s.rpcLayer.Addr().String())
		s.leaderConn, err = newConnToLeader(newConn, s.rpcLayer.Addr().String(), s.lg)
		if err != nil {
			s.lg.Printf("Got error connecting to leader %s from follower %s : %s", s.Leader().String(), s.rpcLayer.Addr().String(), err)
			return nil, err
		}
		s.lg.Printf("Connected to leader, reported addr %s connected addr %s", s.Leader(), s.leaderConn.remoteAddr())
	}
	s.lg.Printf("Creating new command in dispatchToLeader")
	cb := s.state.newCommand()
	s.lg.Printf("Forwarding command in dispatchToLeader")
	err = s.leaderConn.forwardCommand(cb, cmd, args)
	if err != nil {
		cb.cancel()
		return nil, err
	}
	return cb, nil
}

func (s *server) Read() (*mdb.Txn, error) {
	return s.state.ReadTxn()
}

func (s *server) Rsync() error {
	resultCh := s.Command("Noop", [][]byte{})
	result := <-resultCh
	return result.Err
}
func (s *server) Close() error {
	f := s.raft.Shutdown()
	return f.Error()
}
