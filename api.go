//
package flotilla

import (
	mdb "github.com/jbooth/gomdb"
	"net"
)

// Represents a DB.
// DBs work as peers in a cluster.  At any given time, some peer is the leader
// and all updates move through that leader in single-threaded fashion and distributed
// to followers in a consistent ordering.
// Read operations are done against local data.
type DB interface {
	// Opens a read transaction from our local copy of the database
	// This transaction represents a snapshot in time.  Concurrent and
	// subsequent writes will not affect it or be visible.
	//
	// Each Txn opened is guaranteed to be able to see the results of any
	// command, cluster-wide, that completed before the last successful command
	// executed from this node.
	//
	// If we are not leader, this command will execute a no-op command through raft before
	// returning a new txn in order to guarantee happens-before ordering
	// for anything that reached the leader before we called Read()
	Read() (*mdb.Txn, error)

	// Executes a user defined command on the leader of the cluster, wherever that may be.
	// Commands are executed in a fixed, single-threaded order on the leader
	// and propagated to the cluster in a log where they are applied.
	// Returns immediately, but Result will not become available on the returned chan until
	// the command has been processed on the leader and replicated on this node.
	//
	// Visibility:  Upon receiving a successful Result from the returned channel,
	// our command and all previously successful commands cluster-wide
	// have been committed to the leader and to local storage,
	// and will be visible to all future Read() Txns on this node or the leader.
	// Other nodes aside from this node and the leader are guaranteed to execute
	// all commands in the same order, guaranteeing consistency with concurrent
	// commands across the cluster, but are not necessarily guaranteed
	// to have received this command yet.
	Command(cmdName string, args [][]byte) <-chan Result

	// returns true if we're leader, supplied for optimization purposes only.
	// you should not be polling this for correctness reasons, all state changes should happen as Commands
	IsLeader() bool

	// leader addr, same disclaimer as IsLeader()
	Leader() net.Addr

	// shuts down this instance
	Close() error
}

// we implement a few standard utility ops on top of the BaseDB
type DefaultOpsDB interface {
	DB
	// built-in Put command, returns empty bytes
	Put(dbName string, key, val []byte) <-chan Result
	// built-in Remove command, returns empty bytes
	Remove(dbName string, key []byte) <-chan Result
	// built-in PutIfAbsent command, returns [1] if put succeeded, [0] if not
	PutIfAbsent(dbName string, key, val []byte) <-chan Result
	// built-in Compare and Swap command, returns value after execution
	CompareAndSwap(dbName string, key, expectedVal, setVal []byte) <-chan Result
	// built-in Compare and Remove, returns [1] if removed, [0] if not
	CompareAndRemove(dbName string, key, expectedVal []byte) <-chan Result
	// No-op command, useful for creating a happens-before barrier, returns empty bytes
	Barrier() <-chan Result
}

// Commands are registered with members of the cluster on startup
// Do not leak txn handles or cursor handles outside of the execution of a command.
// Keep these consistent across machines!  Consider using versioning in your
// command names.
type Command func(args [][]byte, txn *mdb.Txn) ([]byte, error)

type Result struct {
	Response []byte
	Err      error
}
