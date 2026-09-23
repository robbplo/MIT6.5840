package raft

import (
	"time"

	"6.5840/labrpc"
)

const rpcRetryDelay = 10 * time.Millisecond
const rpcRetries = 2

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	LogLen        int // length of the follower's log
	ConflictTerm  int // term where conflict occurred, 0 if no conflict
	ConflictIndex int // index of the first entry with conflicting term
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	r := make(chan *AppendEntriesReply)
	rf.appendRequests <- appendRequest{args: *args, reply: r}
	*reply = *<-r
}

func (rf *Raft) AppendEntriesRPC(serverId int, args AppendEntriesArgs) {
	go func() {
		attempts := 0
		for attempts <= rpcRetries {
			attempts++
			reply := AppendEntriesReply{}
			ok := rf.peers[serverId].Call("Raft.AppendEntries", args, &reply)
			if ok {
				rf.appendReplies <- appendReply{
					ok:       true,
					serverId: serverId,
					args:     args,
					reply:    reply,
				}
				return
			}
			time.Sleep(rpcRetryDelay)
		}
		rf.appendReplies <- appendReply{
			ok:       false,
			serverId: serverId,
			args:     args,
		}
	}()
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
	rf.voteRequests <- voteRequest{args: *args, reply: r}
	*reply = *<-r
}

func (rf *Raft) RequestVoteRPC(server *labrpc.ClientEnd, args RequestVoteArgs) {
	go func() {
		attempts := 0
		for attempts <= rpcRetries {
			attempts++
			reply := RequestVoteReply{}
			ok := server.Call("Raft.RequestVote", args, &reply)
			if ok {
				rf.voteReplies <- voteReply{ok: true, args: args, reply: reply}
				return
			}
			time.Sleep(rpcRetryDelay)
		}
		rf.voteReplies <- voteReply{ok: false, args: args}
	}()
}
