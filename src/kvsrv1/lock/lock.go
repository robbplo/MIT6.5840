package lock

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck         kvtest.IKVClerk
	mu         sync.Mutex
	name       string
	identifier string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	lk.name = lockname
	lk.identifier = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, _ := lk.ck.Get(lk.name)
		if value == lk.identifier {
			return
		}
		if value == "" {
			err := lk.ck.Put(lk.name, lk.identifier, version)
			if err == rpc.OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	value, version, error := lk.ck.Get(lk.name)
	if error == rpc.ErrNoKey {
		return
	}
	if value != lk.identifier {
		return
	}
	lk.ck.Put(lk.name, "", version)
}
