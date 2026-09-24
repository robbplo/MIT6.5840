package raft

import (
	"6.5840/labrpc"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex logIndex
	PrevLogTerm  int
	Entries      []entry
	LeaderCommit logIndex
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	LastLogIndex  logIndex // length of the follower's log
	ConflictTerm  int      // term where conflict occurred, 0 if no conflict
	ConflictIndex logIndex // index of the first entry with conflicting term
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	r := make(chan *AppendEntriesReply)
	rf.appendRequests <- appendRequest{args: *args, reply: r}
	*reply = *<-r
}

func (rf *Raft) AppendEntriesRPC(serverId int, args AppendEntriesArgs) {
	go func() {
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
	LastLogIndex logIndex
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
		reply := RequestVoteReply{}
		ok := server.Call("Raft.RequestVote", args, &reply)
		if ok {
			rf.voteReplies <- voteReply{ok: true, args: args, reply: reply}
			return
		}
		rf.voteReplies <- voteReply{ok: false, args: args}
	}()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex logIndex
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	r := make(chan InstallSnapshotReply)
	rf.installRequests <- installRequest{args: args, reply: r}
	*reply = <-r
}

func (rf *Raft) InstallSnapshotRPC(serverId int, args InstallSnapshotArgs) {
	go func() {
		reply := InstallSnapshotReply{}
		ok := rf.peers[serverId].Call("Raft.InstallSnapshot", args, &reply)
		if ok {
			rf.installReplies <- installReply{ok: true, serverId: serverId, args: args, reply: reply}
			return
		}
		rf.installReplies <- installReply{ok: false, serverId: serverId, args: args}
	}()
}
