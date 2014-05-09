//
package flotilla

import (
	"github.com/jbooth/flotilla/mdb"
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
	Read() (Txn, error)

	// Executes a command on the leader of the cluster, wherever that may be.
	// Commands are executed in a fixed, single-threaded order on the leader
	// and propagated to the cluster in a log where they are applied.
	// Result will not become available until the command has been processed
	// on this node.
	//
	// Visibility:  Upon receiving a successful Result from the returned channel,
	// our command and all previously successful commands cluster-wide
	// have been committed to local storage, and will be visible to all future
	// Read() Txns on this node.
	// Other nodes aside from this node and the master are guaranteed to execute
	// all commands in the same order, guaranteeing consistency with concurrent
	// commands across the cluster, but are not guaranteed to have received
	// this command yet.
	Command(cmdName string, args [][]byte) (<-chan Result, error)

	// Issues a no-op command to the leader and blocks until we've received it.
	// On success, any previous command cluster-wide which the leader processed
	// before receiving our Rsync request is guaranteed to be visible
	// to future Read() Txns.
	Rsync() error

	// shuts down this instance
	Close() error
}

// Commands are registered with members of the cluster on startup
// Keep these consistent across machines!  Use versioning in your command
// names
type Command func(args [][]byte, txn WriteTxn) ([]byte, error)

type Result struct {
	Response []byte
	Err      error
}

// DBIOpen Database Flags
const (
	MDB_REVERSEKEY = mdb.REVERSEKEY // use reverse string keys
	MDB_DUPSORT    = mdb.DUPSORT    // use sorted duplicates
	MDB_INTEGERKEY = mdb.INTEGERKEY // numeric keys in native byte order. The keys must all be of the same size.
	MDB_DUPFIXED   = mdb.DUPFIXED   // with DUPSORT, sorted dup items have fixed size
	MDB_INTEGERDUP = mdb.INTEGERDUP // with DUPSORT, dups are numeric in native byte order
	MDB_REVERSEDUP = mdb.REVERSEDUP // with DUPSORT, use reverse string dups
	MDB_CREATE     = mdb.CREATE     // create DB if not already existing
)

// put flags
const (
	MDB_NODUPDATA   = mdb.NODUPDATA
	MDB_NOOVERWRITE = mdb.NOOVERWRITE
	MDB_RESERVE     = mdb.RESERVE
	MDB_APPEND      = mdb.APPEND
	MDB_APPENDDUP   = mdb.APPENDDUP
)

// represents a handle to a named database
type DBI uint

type Txn interface {
	//Open a database in the environment.
	//
	//A database handle denotes the name and parameters of a database,
	//independently of whether such a database exists.
	//The database handle may be discarded by calling #mdb_dbi_close().
	//The old database handle is returned if the database was already open.
	//The handle must only be closed once.
	//The database handle will be private to the current transaction until
	//the transaction is successfully committed. If the transaction is
	//aborted the handle will be closed automatically.
	//After a successful commit the
	//handle will reside in the shared environment, and may be used
	//by other transactions. This function must not be called from
	//multiple concurrent transactions. A transaction that uses this function
	//must finish (either commit or abort) before any other transaction may
	//use this function.
	//
	//To use named databases (with name != NULL), #mdb_env_set_maxdbs()
	//must be called before opening the environment.
	//@param[in] txn A transaction handle returned by #mdb_txn_begin()
	//@param[in] name The name of the database to open. If only a single
	//database is needed in the environment, this value may be NULL.
	//@param[in] flags Special options for this database. This parameter
	//must be set to 0 or by bitwise OR'ing together one or more of the
	//values described here.
	//<ul>
	//<li>#MDB_REVERSEKEY
	//Keys are strings to be compared in reverse order, from the end
	//of the strings to the beginning. By default, Keys are treated as strings and
	//compared from beginning to end.
	//<li>#MDB_DUPSORT
	//Duplicate keys may be used in the database. (Or, from another perspective,
	//keys may have multiple data items, stored in sorted order.) By default
	//keys must be unique and may have only a single data item.
	//<li>#MDB_INTEGERKEY
	//Keys are binary integers in native byte order. Setting this option
	//requires all keys to be the same size, typically sizeof(int)
	//or sizeof(size_t).
	//<li>#MDB_DUPFIXED
	//This flag may only be used in combination with #MDB_DUPSORT. This option
	//tells the library that the data items for this database are all the same
	//size, which allows further optimizations in storage and retrieval. When
	//all data items are the same size, the #MDB_GET_MULTIPLE and #MDB_NEXT_MULTIPLE
	//cursor operations may be used to retrieve multiple items at once.
	//<li>#MDB_INTEGERDUP
	//This option specifies that duplicate data items are also integers, and
	//should be sorted as such.
	//<li>#MDB_REVERSEDUP
	//This option specifies that duplicate data items should be compared as
	//strings in reverse order.
	//<li>#MDB_CREATE
	//Create the named database if it doesn't exist. This option is not
	//allowed in a read-only transaction or a read-only environment.
	//</ul>
	//@param[out] dbi Address where the new #MDB_dbi handle will be stored
	//Some possible errors are:
	//<ul>
	//<li>#MDB_NOTFOUND - the specified database doesn't exist in the environment
	//and #MDB_CREATE was not specified.
	//<li>#MDB_DBS_FULL - too many databases have been opened. See #mdb_env_set_maxdbs().
	//</ul>
	DBIOpen(name string, flags uint) (DBI, error)

	//Empty or delete+close a database.
	//
	//@param[in] txn A transaction handle returned by #mdb_txn_begin()
	//@param[in] dbi A database handle returned by #mdb_dbi_open()
	//@param[in] del 0 to empty the DB, 1 to delete it from the
	//environment and close the DB handle.
	Drop(dbi DBI, del int) error

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
	Get(dbi DBI, key []byte) ([]byte, error)

	//Create a cursor handle for iteration of the database.
	//
	//A cursor is associated with a specific transaction and database.
	//New cursors start at the first key in the database by sort order.
	//A cursor cannot be used when its database handle is closed.  Nor
	//when its transaction has ended. It can be discarded with cursor.Close().
	//A cursor must be closed explicitly, before or after its transaction ends.
	CursorOpen(dbi DBI) (Cursor, error)
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
	//@param[in] cursor A cursor handle returned by #mdb_cursor_open()
	//@param[out] countp Address where the count will be stored
	//@return A non-zero error value on failure and 0 on success. Some possible
	//errors are:
	//<ul>
	//<li>EINVAL - cursor is not initialized, or an invalid parameter was specified.
	//</ul>
	Count() (uint64, error)

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
	Put(dbi DBI, key []byte, val []byte, flags uint) error

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
	Del(dbi DBI, key, val []byte) error
}
