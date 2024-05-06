package concurrency

//
//  #include <../../concurrency/c/future_c.h>
//
//  extern void unlockMutex(void*);
//
//  void go_callback(Future* f, void* m) {
//      unlockMutex(m);
//  }
//
import "C"

import (
	"runtime"
	"sync"
	"unsafe"
)

var _ futureNoVal = (*futureNoValImpl)(nil)

type futureNoVal interface {
	BlockUntilReady()

	IsReady() bool

	Cancel()
}

type Future[V any] interface {
	futureNoVal

	Get() (V, error)
}

type futureNoValImpl struct {
	inner *C.Future
}

func (f *futureNoValImpl) BlockUntilReady() {
	defer runtime.KeepAlive(f)

	if C.future_is_ready(f.inner) == 0 {
		return
	}

	m := &sync.Mutex{}
	m.Lock()
	C.go_set_callback(f.inner, unsafe.Pointer(m))
	m.Lock()
}

func (f *futureNoValImpl) IsReady() bool {
	defer runtime.KeepAlive(f)

	return C.future_is_ready(f.inner) != 0
}

func (f *futureNoValImpl) Cancel() {
	defer runtime.KeepAlive(f)

	C.future_cancel(f.inner)
}

// unlockMutex is exported to C so that it can be called from C code.
//
//export unlockMutex
func unlockMutex(p unsafe.Pointer) {
	m := (*sync.Mutex)(p)
	m.Unlock()
}
