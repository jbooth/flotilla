
type JoinCommand struct {
	Name             string `json:"name"`
	ConnectionString string `json:"connectionString"`
}

// The name of the Join command in the log
func (c *JoinCommand) CommandName() string {
	return "join"
}

func (c *JoinCommand) Apply(server raft.Server) (interface{}, error) {
	err := server.AddPeer(c.Name, c.ConnectionString)
	if err != nil {
		return nil, err
	}

	clusterConfig := server.Context().(*cluster.ClusterConfiguration)

	newServer := clusterConfig.GetServerByRaftName(c.Name)
	// it's a new server the cluster has never seen, make it a potential
	if newServer != nil {
		return nil, fmt.Errorf("Server %s already exist", c.Name)
	}

	log.Info("Adding new server to the cluster config %s", c.Name)
	clusterServer := cluster.NewClusterServer(c.Name,
		c.ConnectionString,
		c.ProtobufConnectionString,
		nil,
		clusterConfig.GetLocalConfiguration())
	clusterConfig.AddPotentialServer(clusterServer)
	return nil, nil
}