package flotilla

import (
	"fmt"
	mdb "github.com/jbooth/gomdb"
	"sync"
)

// wrapper for mdb env that waits until all outstanding read txns closed
// before closing env
type env struct {
	e           *mdb.Env
	l           *sync.Mutex
	numOpen     uint64
	shouldClose bool
	closed      bool
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
	// disable fsync because we are guaranteeing consistency/durability via raft snapshotting and edit logs
	err = e.Open(filePath, mdb.WRITEMAP|mdb.NOSYNC|mdb.NOTLS, 0766)
	if err != nil {
		e.Close()
		return nil, err
	}
	return &env{e, new(sync.Mutex), 0, false, false}, nil
}

// opens a read transaction,
func (e *env) readTxn() (*mdb.Txn, error) {
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
	return t, nil
}

// opens a write transaction, we don't track numOpen with these because
// it shouldn't be called at the same time as a snapshot
func (e *env) writeTxn() (*mdb.Txn, error) {
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
	return t, nil
}

// aborts transactionsuppressing any errors, and frees
func (e *env) CloseTransaction(t *mdb.Txn) error {
	t.Abort() // ignore error
	e.l.Lock()
	defer e.l.Unlock()
	e.numOpen--
	if e.shouldClose && e.numOpen == 0 && !e.closed {
		e.e.Close()
		e.closed = true
	}
	e.e.Close()
	return nil
}

// marks this env as pending-closed.  closing the last open transaction will close
// env handle
func (e *env) Close() error {
	e.l.Lock()
	defer e.l.Unlock()
	e.shouldClose = true
	e.e.Sync(1)
	e.e.Close()
	return nil
}
