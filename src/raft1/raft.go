package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"os"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const heartbeatInterval = 50 * time.Millisecond

type role uint8

const (
	follower role = iota
	candidate
	leader
)

type entry struct {
	Term    int
	Command any
}

// A Go object implementing a single Raft peer.
type Raft struct {
	// mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Persistent raft state
	currentTerm int
	votedFor    *int
	log         []entry

	// volatile raft state
	commitIndex int
	lastApplied int

	// volatile leader state
	nextIndex  []int
	matchIndex []int

	role     role
	applyCh  chan raftapi.ApplyMsg // lab-specific commit channel
	leaderId *int                  // currently known leader, for client redirect

	electionVotes int
	electionTerm  int
	electionTimer *time.Timer

	heartbeatTicker *time.Ticker // leader heartbeat ticker

	// actor request and reply channels
	stateRequests chan stateReq

	appendRequests chan appendReq
	appendReplies  chan AppendEntriesReply

	voteRequests chan voteReq
	voteReplies  chan RequestVoteReply
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
	electionGoal := (len(rf.peers) + 1) / 2
	for true {
		select {
		case req := <-rf.stateRequests:
			req.c <- raftState{
				isLeader:    rf.role == leader,
				currentTerm: rf.currentTerm,
			}
		case req := <-rf.appendRequests:
			rf.handleAppendEntries(req)
		case req := <-rf.voteRequests:
			rf.handleVoteRequest(req)
		case rep := <-rf.voteReplies:
			if rf.role == candidate {
				if rf.currentTerm >= rep.Term && rep.VoteGranted {
					rf.electionVotes++
				}
				if rf.electionVotes >= electionGoal {
					rf.becomeLeader()
				}
			}
		case <-rf.heartbeatTicker.C:
			if rf.role == leader {
				rf.sendHeartbeats()
			}
		case <-rf.electionTimer.C:
			if rf.role != leader {
				rf.becomeCandidate()
				rf.resetElectionTimer()
			}
		}
	}
}

func (rf *Raft) handleAppendEntries(req appendReq) {
	args := req.args
	reply := AppendEntriesReply{}

	reply.Term = rf.currentTerm
	reply.Success = false
	// request came from old leader, reject
	if args.Term < rf.currentTerm {
		return
	}
	rf.resetElectionTimer()
	// request came from new leader, update
	if args.Term > rf.currentTerm {
		rf.setCurrentTerm(args.Term)
		rf.leaderId = &args.LeaderId
		rf.becomeFollower()
	}

	// // log inconsistency
	// if args.PrevLogIndex < len(rf.log)-1 || args.PrevLogTerm != rf.log[args.PrevLogIndex].term {
	// 	rf.log = rf.log[:args.PrevLogIndex-1]
	// 	return
	// }
	//
	// reply.Success = true
	// rf.log = append(rf.log, args.Entries...)
	// if args.LeaderCommit > rf.commitIndex {
	// 	rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	// }
	req.reply <- &reply
}

func (rf *Raft) handleVoteRequest(req voteReq) {
	args := req.args
	reply := RequestVoteReply{}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// request came from old leader, reject
	if args.Term < rf.currentTerm {
		rf.debugPrint("refused to vote for %v in term %v", args.CandidateId, args.Term)
		return
	}
	// request came from new candidate
	if args.Term > rf.currentTerm {
		rf.setCurrentTerm(args.Term)
		rf.becomeFollower()
	}
	if rf.votedFor != nil && *rf.votedFor != args.CandidateId {
		return
	}
	// grant vote if candidate's log is at least as up-to-date as own log
	lastLogIndex := -1
	lastLogTerm := -1
	if len(rf.log) > 0 {
		lastLogIndex = len(rf.log) - 1
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	if args.LastLogTerm > lastLogTerm || args.LastLogIndex >= lastLogIndex {
		rf.votedFor = &args.CandidateId
		reply.VoteGranted = true
	}
	rf.debugPrint("voted for %v in term %v", args.CandidateId, args.Term)
	req.reply <- &reply
}

func (rf *Raft) becomeFollower() {
	rf.role = follower
}

func (rf *Raft) becomeCandidate() {
	rf.role = candidate
	rf.currentTerm++
	rf.votedFor = &rf.me
	rf.electionVotes = 1
	rf.debugPrint("became candidate")
	rf.requestAllVotes()
}

func (rf *Raft) becomeLeader() {
	rf.debugPrint("became leader")
	rf.heartbeatTicker.Reset(heartbeatInterval)
	rf.role = leader
	rf.sendHeartbeats()
}

func (rf *Raft) sendHeartbeats() {
	for _, peer := range rf.peers {
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      rf.log,
			LeaderCommit: 0,
		}
		rf.callAppendEntries(peer, &args)
	}
}

func (rf *Raft) requestAllVotes() {
	for id, srv := range rf.peers {
		if id == rf.me {
			continue
		}
		lastLogIndex := -1
		lastLogTerm := -1
		if len(rf.log) > 0 {
			lastLogIndex = len(rf.log) - 1
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		rf.callRequestVote(srv, &args)
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

func (rf *Raft) resetElectionTimer() {
	electionTimeout := time.Duration(300+(rand.Int63()%300)) * time.Millisecond
	rf.electionTimer.Reset(electionTimeout)
}

func (rf *Raft) setCurrentTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = nil
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
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
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
	rf.role = follower
	rf.applyCh = applyCh
	rf.heartbeatTicker = time.NewTicker(heartbeatInterval)
	rf.electionTimer = time.NewTimer(1 * time.Second)
	rf.resetElectionTimer()

	rf.stateRequests = make(chan stateReq, 1)
	rf.appendRequests = make(chan appendReq, 1)
	rf.appendReplies = make(chan AppendEntriesReply, 1)
	rf.voteRequests = make(chan voteReq, 1)
	rf.voteReplies = make(chan RequestVoteReply, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.actorLoop()

	return rf
}

var debugTime time.Time

func (rf *Raft) debugPrint(format string, a ...any) {
	if debugTime.IsZero() {
		debugTime = time.Now()
	}
	_, debug := os.LookupEnv("RAFT_DEBUG")
	if !debug {
		return
	}
	role := "follower "
	switch rf.role {
	case leader:
		role = "leader   "
	case candidate:
		role = "candidate"
	}

	elapsedTime := time.Since(debugTime).Milliseconds()
	part1 := fmt.Sprintf("%v\t%v\tid:%v term:%v\t", elapsedTime, role, rf.me, rf.currentTerm)
	part2 := fmt.Sprintf(format, a...)
	fmt.Print(part1 + part2 + "\n")
}
