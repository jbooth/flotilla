package flotilla

import (
	//"gomdb"
	"github.com/jbooth/flotilla/raft"
	"log"
	"net"
	"os"
	"os/path"
	"time"
)

// Instantiates a new DB serving the ops provided, using the provided dataDir and listener
// If Peers is empty, we start as leader.  Otherwise, connect to the existing leader.
func NewDB(
	peers []string,
	dataDir string,
	listen net.Listener,
	dialer func(net.Addr, time.Duration) (net.Conn, error),
	ops map[string]Command) (DB, error) {

	raftDir := dataDir + "/" + raft
	mdbDir := dataDir + "/" + mdb
	// make sure dirs exist
	if err := path.MkdirAll(raftDir, 0755); err != nil {
		return nil, err
	}
	if err = path.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	s := &server{
		name:      nil,
		mdbDir:    mdbDir,
		raftDir:   raftDir,
		peers:     peers,
		notLeader: make(chan bool, 1),
		listen:    listen,
	}
	// Read existing name or generate a new one.
	// why, again? commented out

	//if b, err := ioutil.ReadFile(filepath.Join(s.raftDir, "name")); err == nil {
	//	s.name = string(b)
	//} else {
	//	var i uint64
	//	log.Printf("Using rand package to generate raft server name")
	//	rand.Seed(time.Now().UnixNano())
	//	i = uint64(rand.Int())
	//	s.name = fmt.Sprintf("%07x", i)[0:7]
	//	log.Printf("Setting raft name to %s", s.name)
	//	if err = ioutil.WriteFile(filepath.Join(s.raftDir, "name"), []byte(s.name), 0644); err != nil {
	//		return nil, err
	//	}
	//}
	// start raft server
	var transporter TransporterPlus
	s.raftServer, err = raft.NewServer(s.name, s.path, transporter, nil, s.db, "")
	if err != nil {
		log.Fatal(err)
	}
	s.raftServer.Start()

	if leader != "" {
		// Join to leader if specified.

		log.Println("Attempting to join leader:", leader)

		if !s.raftServer.IsLogEmpty() {
			log.Fatal("Cannot join with an existing log")
		}
		if err := s.Join(leader); err != nil {
			log.Fatal(err)
		}

	} else if s.raftServer.IsLogEmpty() {
		// Initialize the server by joining itself.

		log.Println("Initializing new cluster")

		_, err := s.raftServer.Do(&raft.DefaultJoinCommand{
			Name:             s.raftServer.Name(),
			ConnectionString: s.connectionString(),
		})
		if err != nil {
			log.Fatal(err)
		}

	} else {
		log.Println("Recovered from log")
	}
	return s.httpServer.ListenAndServe()
	return s, nil
}

func newRaft(path string, streams raft.StreamLayer) (*raft.Raft, error) {
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
	peers, err := s.raftPeers.Peers()
	if err != nil {
		store.Close()
		return err
	}
	if !raft.PeerContained(peers, trans.LocalAddr()) {
		s.raftPeers.SetPeers(raft.AddUniquePeer(peers, trans.LocalAddr()))
	}

	// Setup the Raft store
	s.raft, err = raft.NewRaft(raft.DefaultConfig(), s.fsm, store, store,
		snapshots, raftPeers, trans)
	if err != nil {
		store.Close()
		trans.Close()
		return err
	}
}

type server struct {
	raft      raft.Raft
	transport TransporterPlus
	name      string
	mdbDir    string
	raftDir   string
	peers     []string
	notLeader chan bool
	listen    net.Listener
}

// public API
func (s *server) Command(cmd string, args [][]byte) <-chan Result {
	if s.raft.State() == raft.Leader {
		value, err := s.raftServer.Do(command)
		if err != nil {
			log.Error("Cannot run command %#v. %s", command, err)
		}
		return value, err
	} else {
		if leader, ok := s.leaderConnectString(); !ok {
			return nil, errors.New("Couldn't connect to the cluster leader...")
		} else {
			return SendCommandToServer(leader, command)
		}
	}
	return nil, nil
}
