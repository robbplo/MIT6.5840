package rsm

import (
	"fmt"
	"sync"
	"time"
	"uuid"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	Me        int
	Id        uuid.UUID
	Operation any // argument to StateMachine.DoOp
}

type opReply struct {
	Ok    bool
	Reply any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	term         int
	replyChans   map[uuid.UUID]chan opReply
	opTerms      map[int][]uuid.UUID
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		term:         1,
		opTerms:      map[int][]uuid.UUID{},
		replyChans:   map[uuid.UUID]chan opReply{},
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go func() {
		for msg := range rsm.applyCh {
			if msg.CommandValid {
				op := msg.Command.(Op)
				reply := rsm.sm.DoOp(op.Operation)

				if op.Me == rsm.me {
					fmt.Println("receive command ", rsm.me)
					replyCh := rsm.getReplyCh(op.Id)
					select {
					case replyCh <- opReply{Ok: true, Reply: reply}:
						rsm.mu.Lock()
						delete(rsm.replyChans, op.Id)
						rsm.mu.Unlock()
						fmt.Println("send ", op)
					default:
						fmt.Printf("channel closed for %v\n", op.Id)
					}
				}
			}
		}
	}()

	go func() {
		for {
			term, _ := rsm.rf.GetState()
			rsm.mu.Lock()
			for term > rsm.term {
				for _, opId := range rsm.opTerms[rsm.term] {
					ch, ok := rsm.replyChans[opId]
					if ok {
						ch <- opReply{Ok: false}
						delete(rsm.replyChans, opId)
					}
				}
				rsm.term++
			}
			rsm.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		}
	}()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	// Your solution needs to handle an rsm leader that has called Start() for a request submitted with Submit() but loses its leadership before the request is committed to the log. One way to do this is for the rsm to detect that it has lost leadership, by noticing that Raft's term has changed or a different request has appeared at the index returned by Start(), and return rpc.ErrWrongLeader from Submit(). If the ex-leader is partitioned by itself, it won't know about new leaders; but any client in the same partition won't be able to talk to a new leader either, so it's OK in this case for the server to wait indefinitely until the partition heals.

	op, replyCh := rsm.makeOp(req)
	_, _, isLeader := rsm.rf.Start(op)

	if !isLeader {
		rsm.mu.Lock()
		delete(rsm.replyChans, op.Id)
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	reply := <-replyCh
	if !reply.Ok {
		return rpc.ErrWrongLeader, nil
	}
	return rpc.OK, reply.Reply

}

func (rsm *RSM) makeOp(req any) (op Op, replyCh chan opReply) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	op = Op{
		Me:        rsm.me,
		Id:        uuid.New(),
		Operation: req,
	}
	rsm.opTerms[rsm.term] = append(rsm.opTerms[rsm.term], op.Id)
	rsm.replyChans[op.Id] = make(chan opReply, 1)
	return op, rsm.replyChans[op.Id]
}

func (rsm *RSM) getReplyCh(opId uuid.UUID) chan opReply {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	return rsm.replyChans[opId]
}
