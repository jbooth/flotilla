package mdb

/*
#cgo LDFLAGS: -L/usr/local/lib -llmdb
#cgo CFLAGS: -I/usr/local

#include <stdlib.h>
#include <stdio.h>
#include <lmdb.h>
*/
import "C"

import (
	"unsafe"
)

// MDB_val
type Val C.MDB_val

// Create a Val that points to p's data. the Val's data must not be freed
// manually and C references must not survive the garbage collection of p (and
// the returned Val).
func Wrap(p []byte) Val {
	if len(p) == 0 {
		return Val(C.MDB_val{})
	}
	return Val(C.MDB_val{
		mv_size: C.size_t(len(p)),
		mv_data: unsafe.Pointer(&p[0]),
	})
}

// If val is nil, a empty slice is retured.
func (val Val) Bytes() []byte {
	return C.GoBytes(val.mv_data, C.int(val.mv_size))
}

// If val is nil, an empty string is returned.
func (val Val) String() string {
	return C.GoStringN((*C.char)(val.mv_data), C.int(val.mv_size))
}
