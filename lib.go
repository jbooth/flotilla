package flotilla

import (
	"fmt"
	mdb "github.com/jbooth/gomdb"
)

// some default commands
func defaultCommands() map[string]Command {
	return map[string]Command{
		"Put":              put,
		"PutIfAbsent":      putIfAbsent,
		"CompareAndSwap":   compareAndSwap,
		"CompareAndRemove": compareAndRemove,
		"Remove":           remove,
		"Noop":             noop,
	}

}

// wrapper for default commands

type dbOps struct {
	DB
}

func (d dbOps) Put(dbName string, key, val []byte) <-chan Result {
	args := [][]byte{[]byte(dbName), key, val}
	return d.Command("Put", args)
}

func (d dbOps) PutIfAbsent(dbName string, key, val []byte) <-chan Result {
	args := [][]byte{[]byte(dbName), key, val}
	return d.Command("PutIfAbsent", args)
}

func (d dbOps) CompareAndSwap(dbName string, key, expectedVal, setVal []byte) <-chan Result {
	args := [][]byte{[]byte(dbName), key, expectedVal, setVal}
	return d.Command("CompareAndSwap", args)
}

func (d dbOps) Remove(dbName string, key []byte) <-chan Result {
	args := [][]byte{[]byte(dbName), key}
	return d.Command("Remove", args)
}

func (d dbOps) CompareAndRemove(dbName string, key, expectedVal []byte) <-chan Result {
	args := [][]byte{[]byte(dbName), key, expectedVal}
	return d.Command("Remove", args)
}

func (d dbOps) Barrier() <-chan Result {
	args := [][]byte{}
	return d.Command("Noop", args)
}

// put
// arg0: dbName
// arg1: key
// arg2: value
// returns nil
func put(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("Put needs 3 arguments!  Got %d args", len(args))
	}
	dbName := string(args[0])
	dbi, err := txn.DBIOpen(&dbName, mdb.CREATE) // create if not exists
	if err != nil {
		txn.Abort()
		return nil, err
	}
	err = txn.Put(dbi, args[1], args[2], 0)
	if err != nil {
		txn.Abort()
		return nil, err
	}
	err = txn.Commit()
	return nil, err
}

// put if not already set
// arg0:  dbName
// arg1:  key
// arg2:  value
// return:  [1] if added, [0] otherwise
func putIfAbsent(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	dbName := string(args[0])
	dbi, err := txn.DBIOpen(&dbName, mdb.CREATE) // create if not exists
	if err != nil {
		txn.Abort()
		return nil, err
	}
	err = txn.Put(dbi, args[1], args[2], mdb.NOOVERWRITE)
	if err == mdb.KeyExist {
		txn.Abort()
		return []byte{0}, nil
	}
	if err != nil {
		txn.Abort()
		return nil, err
	}
	err = txn.Commit()
	return []byte{1}, err
}

// compare and swap
// arg0: dbName
// arg1: key
// arg2: expectedValue
// arg3: newValue
// return: new row contents as []byte
func compareAndSwap(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	dbName := string(args[0])
	dbi, err := txn.DBIOpen(&dbName, mdb.CREATE) // create if not exists
	existingVal, err := txn.Get(dbi, args[1])
	if err != nil && err != mdb.NotFound {
		txn.Abort()
		return nil, err
	}
	if err == mdb.NotFound || bytesEqual(args[2], existingVal) {
		err = txn.Put(dbi, args[1], args[3], 0)
		if err != nil {
			return nil, err
		}
		err = txn.Commit()
		return args[3], err
	} else {
		txn.Abort()
		return existingVal, nil
	}
}

// remove
// arg0: dbName
// arg1: key
// returns nil
func remove(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	dbName := string(args[0])
	dbi, err := txn.DBIOpen(&dbName, mdb.CREATE) // create if not exists
	if err != nil {
		txn.Abort()
		return nil, err
	}
	err = txn.Del(dbi, args[1], nil)
	if err != nil {
		txn.Abort()
		return nil, err
	}
	return nil, txn.Commit()
}

// remove if set to expected val
// arg0: dbName
// arg1: key
// arg2: expectedVal
// ret:  [1] if removed, [0] otherwise
func compareAndRemove(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	dbName := string(args[0])
	dbi, err := txn.DBIOpen(&dbName, mdb.CREATE) // create if not exists
	existingVal, err := txn.Get(dbi, args[1])
	if err != nil && err != mdb.NotFound {
		txn.Abort()
		return nil, err
	}
	if err == mdb.NotFound || bytesEqual(args[2], existingVal) {
		err = txn.Del(dbi, args[1], nil)
		if err != nil {
			return nil, err
		}
		err = txn.Commit()
		return args[3], err
	} else {
		txn.Abort()
		return existingVal, nil
	}
}

func noop(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	return nil, nil
}

func bytesEqual(a []byte, b []byte) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
