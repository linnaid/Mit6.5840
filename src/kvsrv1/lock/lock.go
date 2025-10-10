package lock

import (
	"log"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"time"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	key string
	user string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck: ck,
		user: kvtest.RandValue(8),
		key: l,
	}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.key)
		if err == rpc.OK {
			// 读 value & version
			if value == "" {
				ok := lk.ck.Put(lk.key, lk.user, version)
				if ok == rpc.OK {
					// log.Println("\033[32mResult: OK -> you have acquired the lock.\n\033[0m")
					return
				} else {
					time.Sleep(50 * time.Millisecond)
					continue
				}
			} else if value != lk.user {
				time.Sleep(50 * time.Millisecond)
				continue
			} else if value == lk.user {
				log.Println("\033[32mYou have acquired the lock.\n\033[0m")
				return
			}
		} else if err == rpc.ErrNoKey {
			ok := lk.ck.Put(lk.key, lk.user, 0)
			if ok == rpc.OK {
				log.Println("\033[32mResult: OK -> you have acquired the lock.\n\033[0m")
				return
			} else {
				time.Sleep(50 * time.Millisecond)
				continue
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	value, version, err := lk.ck.Get(lk.key)
	if err == rpc.OK {
		if value == lk.user {
			ok := lk.ck.Put(lk.key, "", version)
			if ok == rpc.OK {
				log.Println("\033[32mResult: OK -> you have released the lock.\n\033[0m")
				return
			}
		}
	}
}
