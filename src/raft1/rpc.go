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
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	r := make(chan *AppendEntriesReply)
	rf.appendRequests <- appendRequest{args: *args, reply: r}
	*reply = *<-r
}

func (rf *Raft) callAppendEntries(serverId int, args AppendEntriesArgs) {
	go func() {
		ok := false
		attempts := 0
		for !ok && attempts <= rpcRetries {
			reply := AppendEntriesReply{}
			ok = rf.peers[serverId].Call("Raft.AppendEntries", args, &reply)
			if ok {
				rf.appendReplies <- appendReply{
					serverId: serverId,
					args:     args,
					reply:    reply,
				}
				return
			}
			time.Sleep(rpcRetryDelay)
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
	rf.voteRequests <- voteReq{args: *args, reply: r}
	*reply = *<-r
}

func (rf *Raft) callRequestVote(server *labrpc.ClientEnd, args RequestVoteArgs) {
	go func() {
		ok := false
		attempts := 0
		for !ok && attempts <= rpcRetries {
			reply := RequestVoteReply{}
			ok = server.Call("Raft.RequestVote", args, &reply)
			if ok {
				rf.voteReplies <- reply
				return
			}
			time.Sleep(rpcRetryDelay)
		}
	}()
}
