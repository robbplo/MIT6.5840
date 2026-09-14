package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Entry struct {
	term    int
	command any
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Persistent raft state
	currentTerm int
	votedFor    *int
	log         []Entry

	// volatile raft state
	commitIndex int
	lastApplied int

	// volatile leader state
	nextIndex  []int
	matchIndex []int

	applyCh        chan raftapi.ApplyMsg // commit channel
	leaderId       *int                  // currently known leader
	heartbeat      chan int
	electionActive bool
	electionWon    chan int
	electionTimer  *time.Timer

	stateRequests  chan stateReq
	appendRequests chan appendReq
	voteRequests   chan voteReq
}

type raftState struct {
	isLeader    bool
	currentTerm int
}

type stateReq struct {
	c chan raftState
}

type appendReq struct {
	args  AppendEntriesArgs
	reply chan *AppendEntriesReply
}

type voteReq struct {
	args  RequestVoteArgs
	reply chan *RequestVoteReply
}

func (rf *Raft) actorLoop() {
	var leaderId *int
	electionCancel := func() {}

	for true {
		select {
		case <-rf.heartbeat:
			rf.resetElectionTimer()
		case <-rf.electionWon:
			rf.resetElectionTimer()
			leaderId = &rf.me
			// start sending heartbeats
		case req := <-rf.stateRequests:
			req.c <- raftState{
				isLeader:    leaderId != nil && *leaderId == rf.me,
				currentTerm: rf.currentTerm,
			}
		case req := <-rf.voteRequests:
			rf.handleRequestVote(req)
		case <-rf.electionTimer.C:
			electionCancel()
			electionCancel = rf.startElection()
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	req := stateReq{c: make(chan raftState)}
	rf.stateRequests <- req
	state := <-req.c

	return state.currentTerm, state.isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.leaderId = &args.LeaderId
	rf.heartbeat <- args.LeaderId

	// new leader is known to this follower
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}

	// log inconsistency
	rf.currentTerm = args.Term
	if args.PrevLogIndex < len(rf.log)-1 || args.PrevLogTerm != rf.log[args.PrevLogIndex].term {
		rf.log = rf.log[:args.PrevLogIndex-1]
		return
	}

	reply.Success = true
	rf.log = append(rf.log, args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := false
	for !ok {
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		time.Sleep(50 * time.Millisecond)
	}
	return ok
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	r := make(chan *RequestVoteReply)
	rf.voteRequests <- voteReq{args: *args, reply: r}
	res := <-r
	reply.Term = res.Term
	reply.VoteGranted = res.VoteGranted
}

func (rf *Raft) handleRequestVote(req voteReq) {
	args := req.args
	reply := RequestVoteReply{}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	rf.currentTerm = args.Term
	if rf.votedFor != nil && *rf.votedFor != args.CandidateId {
		return
	}
	// grant vote if candidate's log is at least as up-to-date as own log
	lastLogIndex := -1
	lastLogTerm := -1
	if len(rf.log) > 0 {
		lastLogIndex = len(rf.log) - 1
		lastLogTerm = rf.log[len(rf.log)-1].term
	}
	if args.LastLogTerm > lastLogTerm || args.LastLogIndex >= lastLogIndex {
		fmt.Printf("%v voted for %v\n", rf.me, args.CandidateId)
		rf.votedFor = &args.CandidateId
		reply.VoteGranted = true
		// TODO handle resetting votedFor
	}
	req.reply <- &reply
}

func (rf *Raft) sendRequestVote(ctx context.Context, server *labrpc.ClientEnd, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := false
	for !ok {
		ok = server.Call("Raft.RequestVote", args, reply)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(50 * time.Millisecond):
			// retry
		}
	}
	return ok
}

func (rf *Raft) startElection() context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	fmt.Printf("start election: %v\n", rf.me)

	rf.currentTerm++
	votechan := make(chan int, len(rf.peers))

	for id, srv := range rf.peers {
		if id == rf.me {
			continue
		}
		lastLogIndex := -1
		lastLogTerm := -1
		if len(rf.log) > 0 {
			lastLogIndex = len(rf.log) - 1
			lastLogTerm = rf.log[len(rf.log)-1].term
		}
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		go func() {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(ctx, srv, &args, &reply)
			if !ok {
				return
			}
			vote := 0
			if reply.VoteGranted {
				vote++
			}
			votechan <- vote
		}()
	}

	go func(target int, won chan int) {
		votes := 1
		for {
			select {
			case v := <-votechan:
				votes += v
				fmt.Printf("%v got vote, now has %v\n", rf.me, votes)
				if votes > target {
					fmt.Printf("won: %v\n", rf.me)
					won <- 1
					return
				}
			case <-ctx.Done():
				fmt.Printf("vote cancelled")
				return
			}
		}
	}((len(rf.peers)+1)/2, rf.electionWon)

	return cancel
}

func (rf *Raft) resetElectionTimer() {
	electionTimeout := time.Duration(300+(rand.Int63()%300)) * time.Millisecond
	rf.electionTimer.Reset(electionTimeout)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command any) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.heartbeat = make(chan int, 1)
	rf.electionWon = make(chan int, 1)
	rf.electionTimer = time.NewTimer(1 * time.Second)
	rf.resetElectionTimer()

	rf.stateRequests = make(chan stateReq)
	rf.voteRequests = make(chan voteReq)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.actorLoop()

	return rf
}
