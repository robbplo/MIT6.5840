package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type kvEntry struct {
	value   string
	version rpc.Tversion
}

const Debug = false

func DPrintf(format string, a ...any) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu   sync.RWMutex
	data map[string]kvEntry
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.data = map[string]kvEntry{}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	DPrintf("Get: (%v)\n", args.Key)
	kv.mu.RLock()
	entry, ok := kv.data[args.Key]
	kv.mu.RUnlock()
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Err = rpc.OK
	reply.Value = entry.value
	reply.Version = entry.version
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	DPrintf("Put: (%v, %v, %v)", args.Key, args.Value, args.Version)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, ok := kv.data[args.Key]
	if ok && entry.version != args.Version {
		reply.Err = rpc.ErrVersion
		return
	}
	if !ok && args.Version != 0 {
		reply.Err = rpc.ErrNoKey
		return
	}
	kv.data[args.Key] = kvEntry{
		value:   args.Value,
		version: args.Version + 1,
	}
	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
