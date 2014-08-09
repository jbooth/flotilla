==
Flotilla

Flotilla is a consensus, embedded and programmable database, intended as a building block for distributed databases.

Consensus -- All changes to a Flotilla database are replicated in a consistent order across all machines in the Flotilla cluster.

Embedded -- All read transactions are from local storage.  Doesn't expose any external network APIs (that's your job).

Programmable -- All write modifications are done via user-defined functions.  This is done to process the same state machine in the same order on all machines.  There are example user-defined functions in lib.go and the examples on this page.

==
Architecture

Flotilla is based on a straightforward combination of the Raft algorithm (link) with the LMDB embedded database.  In Raft, all cluster operations happen in a single, defined order.  Operations don't have to be idempotent, because they'll be executed in the same order in each machine, guaranteeing the same final state.
This means we can give each write transaction a single atomic and consistent view of the database, while allowing simultaneous readers due to LMDB's MVCC mechanisms.

This allows for the following simple API:

```go
// Commands are registered with members of the cluster on startup, and can be executed from 
// any member of the cluster in a consistent fashion.
// 
// Do not leak txn handles or cursor handles outside of the execution of a command.
// Keep these consistent across machines!  Consider using versioning in your
// command names.
type Command func(args [][]byte, txn WriteTxn) ([]byte, error)

type Result struct {
	Response []byte
	Err      error
}

type DB interface {
	// Opens a read transaction from our local copy of the database
	// This transaction represents a snapshot in time.  Concurrent and
	// subsequent writes will not affect it or be visible.
	//
	// Each Txn opened is guaranteed to be able to see the results of any
	// command, cluster-wide, that completed before the last successful command
	// executed from this node.
	//
	// If we are not leader, this method will execute a no-op command through raft before
	// returning a new txn in order to guarantee happens-before ordering
	// for anything that reached the leader before we called Read()
	Read() (Txn, error)

	// Executes a user defined command on the leader of the cluster, wherever that may be.
	// Commands are executed in a fixed, single-threaded order on the leader
	// and propagated to the cluster in a log where they are applied.
	// Returns immediately, but Result will not become available on the returned chan until
	// the command has been processed on the leader and replicated on this node.
	//
	// Visibility:  Upon receiving a successful Result from the returned channel,
	// our command and all previously successful commands from any machine
	// have been committed to the leader and to this machine's local storage,
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
```

See api.go for more details on what you can do with a ReadTxn and a WriteTxn.  (basically, get, scan, put)

See the DB constructors in server.go, tests and hopefully-written-soon examples for how to set up a cluster.
