
// The raftd server is a combination of the Raft server and an HTTP
// server which acts as the transport.
type RaftServer struct {
	name                     string
	path                     string
	bind_address             string
	router                   *mux.Router
	raftServer               raft.Server
	httpServer               *http.Server
	clusterConfig            *cluster.ClusterConfiguration
	mutex                    sync.RWMutex
	listener                 net.Listener
	closing                  bool
	config                   *configuration.Configuration
	notLeader                chan bool
	coordinator              *CoordinatorImpl
	processContinuousQueries bool
}

// Creates a new server.
func NewRaftServer(config *configuration.Configuration, clusterConfig *cluster.ClusterConfiguration) *RaftServer {
	// raft.SetLogLevel(raft.Debug)
	if !registeredCommands {
		registeredCommands = true
		for _, command := range internalRaftCommands {
			raft.RegisterCommand(command)
		}
	}

	s := &RaftServer{
		path:          config.RaftDir,
		clusterConfig: clusterConfig,
		notLeader:     make(chan bool, 1),
		router:        mux.NewRouter(),
		config:        config,
	}
	// Read existing name or generate a new one.
	if b, err := ioutil.ReadFile(filepath.Join(s.path, "name")); err == nil {
		s.name = string(b)
	} else {
		var i uint64
		if _, err := os.Stat("/dev/random"); err == nil {
			log.Info("Using /dev/random to initialize the raft server name")
			f, err := os.Open("/dev/random")
			if err != nil {
				panic(err)
			}
			defer f.Close()
			readBytes := 0
			b := make([]byte, 8)
			for readBytes < 8 {
				n, err := f.Read(b[readBytes:])
				if err != nil {
					panic(err)
				}
				readBytes += n
			}
			err = binary.Read(bytes.NewBuffer(b), binary.BigEndian, &i)
			if err != nil {
				panic(err)
			}
		} else {
			log.Info("Using rand package to generate raft server name")
			rand.Seed(time.Now().UnixNano())
			i = uint64(rand.Int())
		}
		s.name = fmt.Sprintf("%07x", i)[0:7]
		log.Info("Setting raft name to %s", s.name)
		if err = ioutil.WriteFile(filepath.Join(s.path, "name"), []byte(s.name), 0644); err != nil {
			panic(err)
		}
	}

	return s
}

func (s *RaftServer) GetRaftName() string {
	return s.name
}

func (s *RaftServer) leaderConnectString() (string, bool) {
	leader := s.raftServer.Leader()
	peers := s.raftServer.Peers()
	if peer, ok := peers[leader]; !ok {
		return "", false
	} else {
		return peer.ConnectionString, true
	}
}

func (s *RaftServer) doOrProxyCommand(command raft.Command) (interface{}, error) {
	var err error
	var value interface{}
	for i := 0; i < 3; i++ {
		value, err = s.doOrProxyCommandOnce(command)
		if err == nil {
			return value, nil
		}
		if strings.Contains(err.Error(), "node failure") {
			continue
		}
		return nil, err
	}
	return nil, err
}

func (s *RaftServer) doOrProxyCommandOnce(command raft.Command) (interface{}, error) {

	if s.raftServer.State() == raft.Leader {
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

func (s *RaftServer) startRaft() error {
	log.Info("Initializing Raft Server: %s", s.config.RaftConnectionString())

	// Initialize and start Raft server.
	transporter := raft.NewHTTPTransporter("/raft")
	var err error
	s.raftServer, err = raft.NewServer(s.name, s.path, transporter, s.clusterConfig, s.clusterConfig, "")
	if err != nil {
		return err
	}

	s.raftServer.SetElectionTimeout(s.config.RaftTimeout.Duration)
	s.raftServer.LoadSnapshot() // ignore errors

	s.raftServer.AddEventListener(raft.StateChangeEventType, s.raftEventHandler)

	transporter.Install(s.raftServer, s)
	s.raftServer.Start()

	go s.CompactLog()

	if !s.raftServer.IsLogEmpty() {
		log.Info("Recovered from log")
		return nil
	}

	potentialLeaders := s.config.SeedServers

	if len(potentialLeaders) == 0 {
		log.Info("Starting as new Raft leader...")
		name := s.raftServer.Name()
		_, err := s.raftServer.Do(&raft.DefaultJoinCommand{
			Name:             name,
			ConnectionString: s.config.RaftConnectionString(),
		})

		if err != nil {
			log.Error(err)
		}
		err = s.CreateRootUser()
		return err
	}

	for {
		for _, leader := range potentialLeaders {
			log.Info("(raft:%s) Attempting to join leader: %s", s.raftServer.Name(), leader)

			if err := s.Join(leader); err == nil {
				log.Info("Joined: %s", leader)
				return nil
			}
		}

		log.Warn("Couldn't join any of the seeds, sleeping and retrying...")
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}