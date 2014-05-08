package flotilla

import (
	"gomdb"
	"net"
)

// Instantiates a new DB serving the ops provided, using the provided dataDir and listener
// If ClusterAddrs is empty, we start as leader.  Otherwise, connect to the existing leader.
func NewDB(
	clusterAddrs []string, dataDir string, listen net.Listener, ops map[string]func(*ByteArgs) ([]byte, error)) (*DB, error) {

	return nil
}

type DB struct {
	clusterAddrs []string
	dataDir      string
	env          *gomdb.Env
}

type Response struct {
	resp []byte
	err  error
}

func (db *DB) Command(cmd string, args *ByteArgs) <-chan Response {

}
