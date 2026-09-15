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
	"slices"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const heartbeatInterval = 100 * time.Millisecond

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
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Persistent raft state
	currentTerm int
	votedFor    *int
	log         []entry

	commitIndex int // highest entry known committed (init 0, monotonic)
	lastApplied int // highest entry applied to the state machine (init 0, monotonic)

	nextIndex  []int // per server, next entry to send (init leader last log index + 1)
	matchIndex []int // per server, highest entry known replicated (init 0, monotonic)

	role     role
	applyCh  chan raftapi.ApplyMsg // apply a message to the state machine
	leaderId *int                  // currently known leader, for client redirect

	electionVotes int
	electionTerm  int
	electionTimer *time.Timer

	heartbeatTicker *time.Ticker // leader heartbeat ticker

	// actor request and reply channels
	startRequests chan startReq
	stateRequests chan stateReq

	appendRequests chan appendRequest
	appendReplies  chan appendReply

	voteRequests chan voteReq
	voteReplies  chan RequestVoteReply
}

type startReply struct {
	isLeader bool
	index    int
	term     int
}

type startReq struct {
	command any
	reply   chan startReply
}

type stateReply struct {
	isLeader    bool
	currentTerm int
}

type stateReq struct {
	reply chan stateReply
}

type appendRequest struct {
	args  AppendEntriesArgs
	reply chan *AppendEntriesReply
}

type appendReply struct {
	serverId int
	args     AppendEntriesArgs
	reply    AppendEntriesReply
}

type voteReq struct {
	args  RequestVoteArgs
	reply chan *RequestVoteReply
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
	r := make(chan startReply)
	rf.startRequests <- startReq{command: command, reply: r}
	reply := <-r

	return reply.index, reply.term, reply.isLeader
}

func (rf *Raft) actorLoop() {
	majority := (len(rf.peers) + 1) / 2

	for true {
		select {
		// leader
		case req := <-rf.startRequests:
			rf.handleStartRequest(req)
		case rep := <-rf.appendReplies:
			rf.handleAppendReply(rep)
		case <-rf.heartbeatTicker.C:
			if rf.role == leader {
				rf.sendHeartbeats()
			}
		// candidate
		case rep := <-rf.voteReplies:
			if rf.role == candidate {
				if rf.currentTerm >= rep.Term && rep.VoteGranted {
					rf.electionVotes++
				}
				if rf.electionVotes >= majority {
					rf.becomeLeader()
				}
			}
		case <-rf.electionTimer.C:
			if rf.role != leader {
				rf.becomeCandidate()
				rf.resetElectionTimer()
			}
		// follower
		case req := <-rf.appendRequests:
			rf.handleAppendRequest(req)
		case req := <-rf.voteRequests:
			rf.handleVoteRequest(req)
		// misc
		case req := <-rf.stateRequests:
			req.reply <- stateReply{isLeader: rf.role == leader, currentTerm: rf.currentTerm}
		}
	}
}

func (rf *Raft) handleStartRequest(req startReq) {
	if rf.role == leader {
		rf.debugPrint("start command: %v", req.command)
		rf.log = append(rf.log, entry{Command: req.command, Term: rf.currentTerm})
		rf.sendAllAppendRequests()
		rf.heartbeatTicker.Reset(heartbeatInterval)
		req.reply <- startReply{isLeader: true, index: len(rf.log) - 1, term: rf.currentTerm}
	} else {
		req.reply <- startReply{isLeader: false, index: len(rf.log) - 1, term: rf.currentTerm}
	}
}

func (rf *Raft) handleAppendReply(r appendReply) {
	if r.reply.Term > rf.currentTerm {
		rf.becomeFollower()
		return
	}
	if len(r.args.Entries) == 0 {
		return
	}
	if r.reply.Success {
		newLogs := len(r.args.Entries)
		lastIndex := r.args.PrevLogIndex + newLogs
		rf.nextIndex[r.serverId] = lastIndex + 1
		rf.matchIndex[r.serverId] = lastIndex
		rf.debugPrint("server replicated: %v", r.serverId)

		// check if new commit
		rf.matchIndex[rf.me] = len(rf.log)
		majority := (len(rf.peers) + 1) / 2
		matches := make([]int, len(rf.matchIndex))
		copy(matches, rf.matchIndex)
		slices.Sort(matches)
		rf.debugPrint("matches %v", matches)
		N := matches[majority-1]
		if N > rf.commitIndex {
			rf.commitAndApply(N)
		}

		// sort matchIndex
		// take largest majority
		// take lowest N
		// if match > commitIndex: commitIndex = n

		// If some `N > commitIndex` has a majority of `matchIndex[i] ≥ N`
		// and `log[N].term == currentTerm`: set `commitIndex = N` (§5.3, §5.4)

		return
	}
	rf.debugPrint("follower reported inconsistency")
	rf.nextIndex[r.serverId]--
	rf.sendOneAppendRequest(r.serverId)
}

func (rf *Raft) handleAppendRequest(req appendRequest) {
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

	// log inconsistency
	lastLogIndex := len(rf.log) - 1
	// TODO: fix conditional
	if args.PrevLogIndex <= lastLogIndex && args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		rf.debugPrint(
			"log inconsistency, [args index:%v term:%v] [log index:%v term:%v]",
			args.PrevLogIndex,
			args.PrevLogTerm,
			lastLogIndex,
			rf.log[args.PrevLogIndex].Term,
		)
		rf.log = rf.log[:args.PrevLogIndex-1]
		return
	}

	reply.Success = true
	if len(args.Entries) > 0 {
		rf.debugPrint("appending %v logs", len(args.Entries))
		rf.log = append(rf.log, args.Entries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitAndApply(min(args.LeaderCommit, len(rf.log)-1))
	}
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
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
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
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.heartbeatTicker.Reset(heartbeatInterval)
	rf.role = leader
	rf.sendHeartbeats()
}

func (rf *Raft) sendAllAppendRequests() {
	rf.heartbeatTicker.Reset(heartbeatInterval)
	for id := range rf.peers {
		rf.sendOneAppendRequest(id)
	}
}

func (rf *Raft) sendOneAppendRequest(id int) {
	if id == rf.me {
		return
	}
	nextIndex := rf.nextIndex[id]
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: nextIndex - 1,
		PrevLogTerm:  rf.log[nextIndex-1].Term,
		Entries:      rf.log[rf.nextIndex[id]:],
		LeaderCommit: rf.commitIndex,
	}
	rf.callAppendEntries(id, &args)
}

func (rf *Raft) sendHeartbeats() {
	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      []entry{},
			LeaderCommit: rf.commitIndex,
		}
		rf.callAppendEntries(id, &args)
	}
}

func (rf *Raft) requestAllVotes() {
	for id, srv := range rf.peers {
		if id == rf.me {
			continue
		}
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[lastLogIndex].Term
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
	req := stateReq{reply: make(chan stateReply)}
	rf.stateRequests <- req
	state := <-req.reply

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

func (rf *Raft) commitAndApply(newCommitIndex int) {
	rf.debugPrint("committing until %v", newCommitIndex)
	// skip dummy log at index 0
	startIndex := max(rf.commitIndex, 1)
	for i := startIndex; i <= newCommitIndex; i++ {
		rf.debugPrint("applying log i:%v cmd:%v", i, rf.log[i].Command)

		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			CommandIndex: i,
			Command:      rf.log[i].Command,
		}
	}
	rf.commitIndex = newCommitIndex
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
	rf.log = []entry{{Term: 0, Command: 0}}
	rf.applyCh = applyCh
	rf.heartbeatTicker = time.NewTicker(heartbeatInterval)
	rf.electionTimer = time.NewTimer(1 * time.Second)
	rf.resetElectionTimer()

	rf.startRequests = make(chan startReq)
	rf.stateRequests = make(chan stateReq, 1)
	rf.appendRequests = make(chan appendRequest, 1)
	rf.appendReplies = make(chan appendReply, 1)
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
