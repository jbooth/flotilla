//
package flotilla

import (
	"github.com/jbooth/flotilla/mdb"
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
	Read() (Txn, error)

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
	CompareAndRemove(dbNAme string, key, expectedVal []byte) <-chan Result
	// No-op command, useful for creating a happens-before barrier, returns empty bytes
	Barrier() <-chan Result
}

// Commands are registered with members of the cluster on startup
// Do not leak txn handles or cursor handles outside of the execution of a command.
// Keep these consistent across machines!  Consider using versioning in your
// command names.
type Command func(args [][]byte, txn WriteTxn) ([]byte, error)

type Result struct {
	Response []byte
	Err      error
}

// DBIOpen Database Flags
const (
	// when opening a DBI, use reverse string keys
	MDB_REVERSEKEY = mdb.REVERSEKEY
	// when opening a DBI, use sorted duplicates
	MDB_DUPSORT = mdb.DUPSORT
	// when opening a DBI, numeric keys in native byte order. The keys must all be of the same size.
	MDB_INTEGERKEY = mdb.INTEGERKEY
	// when opening a DBI, with DUPSORT, sorted dup items have fixed size
	MDB_DUPFIXED = mdb.DUPFIXED
	// when opening a DBI, with DUPSORT, dups are numeric in native byte order
	MDB_INTEGERDUP = mdb.INTEGERDUP
	// when opening a DBI, with DUPSORT, use reverse string dups
	MDB_REVERSEDUP = mdb.REVERSEDUP
	// when opening a DBI, create DB if not already existing
	MDB_CREATE = mdb.CREATE
)

// put flags
const (
	// when putting a new key/data pair, enter pair only if it does not
	//already appear in the database. This flag may only be specified
	//if the database was opened with #MDB_DUPSORT. The function will
	//return #MDB_KEYEXIST if the key/data pair already appears in the
	//database.
	MDB_NODUPDATA   = mdb.NODUPDATA
	MDB_NOOVERWRITE = mdb.NOOVERWRITE
	MDB_RESERVE     = mdb.RESERVE
	MDB_APPEND      = mdb.APPEND
	MDB_APPENDDUP   = mdb.APPENDDUP
)

// A txn represents an atomic snapshot view of the database.
// A read Txns' view of the data will not be changed by concurrent writes.
// Txns are NOT thread safe, do not share between goroutines.  Open a new one if reading,
// or dispatch another command if writing.
type Txn interface {
	//Open a database in the environment.  Name can be nil.
	//Possible flags:  MDB_CREATE, MDB_REVERSEKEY, MDB_DUPSORT, MDB_INTEGERKEY, MDB_DUPFIXED, MDB_INTEGERDUP, MDB_REVERSEDUP
	//
	//Returns type mdb.DBI, an opaque uint representing this open DB
	//Some possible errors are:
	//
	//MDB_NOTFOUND - the specified database doesn't exist in the environment and #MDB_CREATE was not specified.
	//MDB_DBS_FULL - too many databases have been opened. See #mdb_env_set_maxdbs().
	DBIOpen(name *string, flags uint) (mdb.DBI, error)

	//Empty or delete+close a database.
	//Values for del: 0 to empty the DB, 1 to delete it from the
	//environment and close the DB handle.
	Drop(dbi mdb.DBI, del int) error

	//Get items from a database.
	//
	//This function retrieves key/data pairs from the database.
	//If the database supports duplicate keys (#MDB_DUPSORT) then the
	//first data item for the key will be returned. Retrieval of other
	//items requires the use of #mdb_cursor_get().
	//
	//Note: The memory pointed to by the returned values is owned by the
	//database and points to memory-mapped storage.
	//Values returned from the database are valid only until a
	//subsequent update operation, or the end of the transaction.
	//The caller may not
	//modify it in any way. For values returned in a read-only transaction
	//any modification attempts will cause a SIGSEGV.
	//Some possible errors are:
	//    mdb.NOTFOUND - they key was not in the database
	//    syscall.EINVAL - an invalid parameter was specified.
	Get(dbi mdb.DBI, key []byte) ([]byte, error)

	//Create a cursor handle for iteration of the database.
	//
	//A cursor is associated with a specific transaction and database.
	//New cursors start at the first key in the database by sort order.
	//A cursor cannot be used when its database handle is closed.  Nor
	//when its transaction has ended. It can be discarded with cursor.Close().
	//A cursor must be closed explicitly, before or after its transaction ends.
	CursorOpen(dbi mdb.DBI) (Cursor, error)

	// close this transaction.  if we're a write transaction, this discards pending changes.
	Abort()
}

type Cursor interface {
	// Returns the next key/value from this cursor.
	// If set_key is non-nil and length > 0, we will seek to
	// the location of set_key and return new values from there.
	Get(set_key []byte, op uint) (key, val []byte, err error)

	//Return count of duplicates for current key.
	//
	//This call is only valid on databases that support sorted duplicate
	//data items #MDB_DUPSORT.
	Count() (uint64, error)

	// close this cursor
	Close() error
}

type WriteTxn interface {
	Txn

	//Store items into a database.
	//
	//This function stores key/data pairs in the database. The default behavior
	//is to enter the new key/data pair, replacing any previously existing key
	//if duplicates are disallowed, or adding a duplicate data item if
	//duplicates are allowed (#MDB_DUPSORT).
	//@param[in] txn A transaction handle returned by #mdb_txn_begin()
	//@param[in] dbi A database handle returned by #mdb_dbi_open()
	//@param[in] key The key to store in the database
	//@param[in,out] data The data to store
	//@param[in] flags Special options for this operation. This parameter
	//must be set to 0 or by bitwise OR'ing together one or more of the
	//values described here.
	//<ul>
	//<li>#MDB_NODUPDATA - enter the new key/data pair only if it does not
	//already appear in the database. This flag may only be specified
	//if the database was opened with #MDB_DUPSORT. The function will
	//return #MDB_KEYEXIST if the key/data pair already appears in the
	//database.
	//<li>#MDB_NOOVERWRITE - enter the new key/data pair only if the key
	//does not already appear in the database. The function will return
	//#MDB_KEYEXIST if the key already appears in the database, even if
	//the database supports duplicates (#MDB_DUPSORT). The \b data
	//parameter will be set to point to the existing item.
	//<li>#MDB_RESERVE - reserve space for data of the given size, but
	//don't copy the given data. Instead, return a pointer to the
	//reserved space, which the caller can fill in later - before
	//the next update operation or the transaction ends. This saves
	//an extra memcpy if the data is being generated later.
	//MDB does nothing else with this memory, the caller is expected
	//to modify all of the space requested.
	//<li>#MDB_APPEND - append the given key/data pair to the end of the
	//database. No key comparisons are performed. This option allows
	//fast bulk loading when keys are already known to be in the
	//correct order. Loading unsorted keys with this flag will cause
	//data corruption.
	//<li>#MDB_APPENDDUP - as above, but for sorted dup data.
	//</ul>
	//@return A non-zero error value on failure and 0 on success. Some possible
	//errors are:
	//<ul>
	//<li>#MDB_MAP_FULL - the database is full, see #mdb_env_set_mapsize().
	//<li>#MDB_TXN_FULL - the transaction has too many dirty pages.
	//<li>EACCES - an attempt was made to write in a read-only transaction.
	//<li>EINVAL - an invalid parameter was specified.
	//</ul>
	Put(dbi mdb.DBI, key []byte, val []byte, flags uint) error

	//Delete items from a database.
	//
	//This function removes key/data pairs from the database.
	//If the database does not support sorted duplicate data items
	//(#MDB_DUPSORT) the data parameter is ignored.
	//If the database supports sorted duplicates and the data parameter
	//is NULL, all of the duplicate data items for the key will be
	//deleted. Otherwise, if the data parameter is non-NULL
	//only the matching data item will be deleted.
	//This function will return #MDB_NOTFOUND if the specified key/data
	//pair is not in the database.

	//Some possible errors are:
	//    mdb.NOT_FOUND
	//    syscall.EACCES
	//    syscall.EINVAL
	//<ul>
	//<li>EACCES - an attempt was made to write in a read-only transaction.
	//<li>EINVAL - an invalid parameter was specified.
	//</ul>
	Del(dbi mdb.DBI, key, val []byte) error

	// flush changes to storage and close this transaction
	Commit() error
}
