package raft

import (
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Persistent raft state
	currentTerm int
	votedFor    int
	log         []entry
	snapshot    snapshot

	commitIndex logIndex // highest entry known committed (init 0, monotonic)
	lastApplied logIndex // highest entry applied to the state machine (init 0, monotonic)

	nextIndex      []logIndex // per server, next entry to send (init leader last log index + 1)
	matchIndex     []logIndex // per server, highest entry known replicated (init 0, monotonic)
	appendLastSent []time.Time

	role    role
	applyCh chan raftapi.ApplyMsg // apply a message to the state machine

	electionVotes int
	electionTerm  int
	electionTimer *time.Timer

	heartbeatTicker *time.Ticker // leader heartbeat ticker

	// actor request and reply channels
	startRequests    chan startReq
	snapshotRequests chan snapshotReq
	stateRequests    chan stateReq

	appendRequests chan appendRequest
	appendReplies  chan appendReply

	voteRequests chan voteRequest
	voteReplies  chan voteReply
}

type role uint8

const (
	follower role = iota
	candidate
	leader
)

// Index of a log entry. This includes the last snapshot index.
type logIndex int

type entry struct {
	Term    int
	Command any
}

type snapshot struct {
	LastIndex logIndex
	LastTerm  int
	Data      []byte
}

type startReq struct {
	command any
	reply   chan startReply
}

type startReply struct {
	isLeader bool
	index    logIndex
	term     int
}

type snapshotReq struct {
	index    logIndex
	snapshot []byte
}

type stateReq struct {
	reply chan stateReply
}

type stateReply struct {
	isLeader    bool
	currentTerm int
}

type appendRequest struct {
	args  AppendEntriesArgs
	reply chan *AppendEntriesReply
}

type appendReply struct {
	ok       bool
	serverId int
	args     AppendEntriesArgs
	reply    AppendEntriesReply
}

type voteRequest struct {
	args  RequestVoteArgs
	reply chan *RequestVoteReply
}

type voteReply struct {
	ok    bool
	args  RequestVoteArgs
	reply RequestVoteReply
}
