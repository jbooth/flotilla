package flotilla

import (
	"fmt"
	"github.com/jbooth/flotilla/mdb"
	"sync"
)

// wrapper for mdb env that waits until all outstanding read txns closed
// before closing env
type env struct {
	e           *mdb.Env
	l           *sync.Mutex
	numOpen     uint64
	shouldClose bool
}

func newenv(filePath string) (*env, error) {
	e, err := mdb.NewEnv()
	if err != nil {
		return nil, err
	}
	fmt.Printf("Building new MDB env at %s\n", filePath)
	// TODO make these configurable
	e.SetMaxReaders(1024)
	e.SetMaxDBs(1024)
	e.SetMapSize(32 * 1024 * 1024 * 1024)
	err = e.Open(filePath, 0, 0766)
	if err != nil {
		e.Close()
		return nil, err
	}
	return &env{e, new(sync.Mutex), 0, false}, nil
}

// opens a read transaction,
func (e *env) readTxn() (Txn, error) {
	e.l.Lock()
	defer e.l.Unlock()
	if e.shouldClose {
		// should never happen
		return nil, fmt.Errorf("Environment is marked as closing, no new txns allowed!")
	}
	t, err := e.e.BeginTxn(nil, mdb.RDONLY)
	if err != nil {
		return nil, err
	}
	e.numOpen++
	return &txn{t, e, false}, nil
}

// opens a write transaction, we don't track numOpen with these because
// it shouldn't be called at the same time as a snapshot
func (e *env) writeTxn() (WriteTxn, error) {
	e.l.Lock()
	defer e.l.Unlock()
	if e.shouldClose {
		// should never happen
		return nil, fmt.Errorf("Environment is marked as closing, no new txns allowed!")
	}
	t, err := e.e.BeginTxn(nil, 0)
	if err != nil {
		return nil, err
	}
	return &writeTxn{t, false}, nil
}

// marks this env as pending-closed.  closing the last open transaction will close
// env handle
func (e *env) Close() error {
	e.l.Lock()
	defer e.l.Unlock()
	e.shouldClose = true
	return nil
}

// represents read transactions to an env that will clean up when closed
// write transactions are already guaranteed not to overlap with a db re-init,
// so they're just raw mdb.Txn handles
type txn struct {
	t      *mdb.Txn
	e      *env
	closed bool
}

func (t *txn) DBIOpen(name *string, flags uint) (mdb.DBI, error) {
	return t.t.DBIOpen(name, flags)
}

func (t *txn) Drop(dbi mdb.DBI, del int) error {
	return t.t.Drop(dbi, del)
}
func (t *txn) Get(dbi mdb.DBI, key []byte) ([]byte, error) {
	return t.t.Get(dbi, key)
}

func (t *txn) CursorOpen(dbi mdb.DBI) (Cursor, error) {
	return t.t.CursorOpen(dbi)
}

func (t *txn) Abort() {
	if t.closed {
		return
	}
	t.t.Abort()
	t.e.l.Lock()
	defer t.e.l.Unlock()
	t.e.numOpen--
	if t.e.shouldClose && t.e.numOpen == 0 {
		t.e.e.Close()
	}
	t.closed = true
}

// wraps write Txn methods with some safety around close/abort,
// so we can call multiple times to prevent leaks
type writeTxn struct {
	t      *mdb.Txn
	closed bool
}

func (t *writeTxn) DBIOpen(name *string, flags uint) (mdb.DBI, error) {
	return t.t.DBIOpen(name, flags)
}

func (t *writeTxn) Drop(dbi mdb.DBI, del int) error {
	return t.t.Drop(dbi, del)
}
func (t *writeTxn) Get(dbi mdb.DBI, key []byte) ([]byte, error) {
	return t.t.Get(dbi, key)
}

func (t *writeTxn) CursorOpen(dbi mdb.DBI) (Cursor, error) {
	return t.t.CursorOpen(dbi)
}

func (t *writeTxn) Put(dbi mdb.DBI, key []byte, val []byte, flags uint) error {
	return t.t.Put(dbi, key, val, flags)
}

func (t *writeTxn) Del(dbi mdb.DBI, key, val []byte) error {
	return t.t.Del(dbi, key, val)
}

func (t *writeTxn) Abort() {
	if t.closed {
		return
	}
	t.t.Abort()
	t.closed = true
	return
}

func (t *writeTxn) Commit() error {
	if t.closed {
		return nil
	}
	err := t.t.Commit()
	t.closed = true
	return err
}
