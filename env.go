package flotilla

import (
	"github.com/jbooth/flotilla/mdb"
	"sync"
)

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
	err = e.Open(filePath, 0, 0766)
	if err != nil {
		e.Close()
		return nil, err
	}
	return &env{e, new(sync.Mutex), 0, false}, nil
}

// opens a read transaction,
func (e *env) txn() (Txn, error) {
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
	e.numOpen++
	return &txn{t, e}
}

// marks this env as pending-closed.  closing the last open transaction will close
// env handle
func (e *env) Close() {
	e.l.Lock()
	defer e.l.Unlock()
	e.shouldClose = true
}

// represents read transactions to an env that will clean up when closed
// write transactions are already guaranteed not to overlap with a db re-init,
// so they're just raw mdb.Txn handles
type txn struct {
	t *mdb.Txn
	e *env
}

func (t *txn) DBIOpen(name string, flags uint) (DBI, error) {
	return t.t.DBIOpen(name, flags)
}

func (t *txn) Drop(dbi DBI, del int) error {
	return t.t.Drop(dbi, del)
}
func (t *txn) Get(dbi DBI, key []byte) ([]byte, error) {
	return t.t.Get(dbi, key)
}

func (t *txn) CursorOpen(dbi DBI) (Cursor, error) {
	return t.t.CursorOpen(dbi)
}

func (t *txn) Abort() {
	t.t.Abort()
	t.e.l.Lock()
	defer t.e.l.Unlock()
	t.e.numOpen--
	if t.e.shouldClose && t.e.numOpen == 0 {
		t.e.e.Close()
	}
}
