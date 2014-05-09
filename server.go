package flotilla

import (
	//"gomdb"
	"net"
)

// Instantiates a new DB serving the ops provided, using the provided dataDir and listener
// If ClusterAddrs is empty, we start as leader.  Otherwise, connect to the existing leader.
func NewDB(
	clusterAddrs []string, dataDir string, listen net.Listener, ops map[string]func(*ByteArgs) ([]byte, error)) (DB, error) {

	return nil, nil
}

type server struct {
	clusterAddrs []string
	dataDir      string
	//env          *mdb.Env
}

func (s *server) Command(cmd string, args *ByteArgs) <-chan Result {
	return nil
}
