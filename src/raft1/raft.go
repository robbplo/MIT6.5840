package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const heartbeatInterval = 250 * time.Millisecond
const electionTimeoutBase = 750
const electionTimeoutRandom = 200

func (rf *Raft) actorLoop() {
	for true {
		select {
		// leader
		case req := <-rf.startRequests:
			rf.handleStartRequest(req)
		case rep := <-rf.appendReplies:
			rf.handleAppendReply(rep)
		case rep := <-rf.installReplies:
			rf.handleInstallReply(rep)
		case <-rf.heartbeatTicker.C:
			if rf.role == leader {
				rf.sendAllAppendRequests()
			}
		// candidate
		case r := <-rf.voteReplies:
			rf.handleVoteReply(r)
		case <-rf.electionTimer.C:
			if rf.role != leader {
				rf.becomeCandidate()
			}
			rf.resetElectionTimer()
		// follower
		case req := <-rf.appendRequests:
			rf.handleAppendRequest(req)
		case req := <-rf.voteRequests:
			rf.handleVoteRequest(req)
		case req := <-rf.installRequests:
			rf.handleInstallRequest(req)
		// all
		case req := <-rf.snapshotRequests:
			rf.handleSnapshotRequest(req)

		case req := <-rf.stateRequests:
			req.reply <- stateReply{isLeader: rf.role == leader, currentTerm: rf.currentTerm}
		}
	}
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
	rf.startRequests <- startRequest{command: command, reply: r}
	reply := <-r

	return int(reply.index), reply.term, reply.isLeader
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.snapshotRequests <- snapshotRequest{
		index:    logIndex(index),
		snapshot: slices.Clone(snapshot),
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) handleStartRequest(req startRequest) {
	if rf.role != leader {
		req.reply <- startReply{isLeader: false, index: rf.lastLogIndex(), term: rf.currentTerm}
		return
	}
	rf.debugPrint("client", "start command: %v nextIndex: %v", req.command, rf.nextIndex)
	rf.log = append(rf.log, entry{Command: req.command, Term: rf.currentTerm})
	rf.persist()
	rf.sendAllAppendRequests()
	req.reply <- startReply{isLeader: true, index: rf.lastLogIndex(), term: rf.currentTerm}
}

func (rf *Raft) handleAppendReply(r appendReply) {
	rf.appendLastSent[r.serverId] = time.Time{}
	if !r.ok {
		return
	}
	if r.reply.Term > rf.currentTerm {
		rf.becomeFollower(r.reply.Term)
		return
	}
	if r.args.Term != rf.currentTerm {
		return
	}
	if r.reply.Success {
		lastIndex := r.args.PrevLogIndex + logIndex(len(r.args.Entries))
		rf.nextIndex[r.serverId] = lastIndex + 1
		// update matchIndex only if the replicated log was from currentTerm
		// so that we never commit entry from previous term (figure 8)
		if rf.getLogTerm(lastIndex) == rf.currentTerm {
			rf.matchIndex[rf.me] = rf.lastLogIndex()
			rf.matchIndex[r.serverId] = lastIndex
		}
		if len(r.args.Entries) > 0 {
			rf.debugPrint("replication", "ok append reply from %v, matchIndex: %v", r.serverId, rf.matchIndex)
		}
		// check if new commit
		majority := (len(rf.peers) + 1) / 2
		matches := make([]logIndex, len(rf.matchIndex))
		copy(matches, rf.matchIndex)
		slices.Sort(matches)
		N := matches[majority-1]
		if N > rf.commitIndex {
			rf.commitAndApply(N)
			rf.sendAllAppendRequests()
		}
		return
	}
	if r.reply.ConflictTerm == 0 {
		// no conflict, next index is after the followers last index
		rf.nextIndex[r.serverId] = r.reply.LastLogIndex + 1
	} else {
		// find next index to send to resolve conflict
		termStartIndex := rf.findTermStartIndex(r.reply.ConflictTerm)
		if termStartIndex == -1 {
			rf.debugPrint("inconsistency", "setting nextIndex[%v] to conflict index: %v", r.serverId, r.reply.ConflictIndex)
			rf.nextIndex[r.serverId] = r.reply.ConflictIndex
		} else {
			rf.debugPrint("inconsistency", "setting nextIndex[%v] to term start index: %v", r.serverId, termStartIndex)
			rf.nextIndex[r.serverId] = termStartIndex
		}
	}
	rf.sendOneAppendRequest(r.serverId)
}

func (rf *Raft) handleInstallReply(rep installReply) {
	if !rep.ok {
		return
	}
	if rep.reply.Term > rf.currentTerm {
		rf.becomeFollower(rep.reply.Term)
		return
	}
	id := rep.serverId
	rf.nextIndex[id] = max(rf.nextIndex[id], rep.args.LastIncludedIndex+1)
	rf.matchIndex[id] = max(rf.matchIndex[id], rep.args.LastIncludedIndex)

	rf.debugPrint(
		"replication",
		"install reply from %v for snapshot %v matchIndex: %v",
		rep.serverId,
		rep.args.LastIncludedIndex,
		rf.matchIndex,
	)
}

func (rf *Raft) handleAppendRequest(req appendRequest) {
	args := req.args
	reply := AppendEntriesReply{}

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.LastLogIndex = rf.lastLogIndex()
	// request came from old leader, reject
	if args.Term < rf.currentTerm {
		req.reply <- &reply
		return
	}
	rf.resetElectionTimer()
	// request came from new leader, update
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if args.PrevLogIndex > reply.LastLogIndex {
		rf.debugPrint("replication", "prev log %v not found, requesting more", args.PrevLogIndex)
		req.reply <- &reply
		return
	}

	// if the previous log entry does not match the leader, replace all logs of the wrong term
	// find term start index and request more logs
	myPrevLogTerm := rf.getLogTerm(args.PrevLogIndex)
	if myPrevLogTerm != args.PrevLogTerm {
		conflictIndex := args.PrevLogIndex
		for conflictIndex > rf.snapshot.LastIndex && rf.getLogTerm(conflictIndex) == myPrevLogTerm {
			conflictIndex--
		}
		conflictIndex++
		reply.ConflictTerm = myPrevLogTerm
		reply.ConflictIndex = conflictIndex
		rf.debugPrint(
			"inconsistency",
			"log inconsistency in previous, index: %v leaderTerm: %v, myTerm: %v",
			args.PrevLogIndex,
			args.PrevLogTerm,
			myPrevLogTerm,
		)
		rf.debugPrint("inconsistency", "requesting logs starting at conflict index %v", conflictIndex)
		req.reply <- &reply
		return
	}

	// if an existing entry conflicts with a new one (same index, different terms)
	// delete the exsiting entry and all that follow it
	reply.Success = true

	if len(args.Entries) > 0 {
		rf.setLogEntries(args.Entries, args.PrevLogIndex+1)
		rf.persist()
		if len(args.Entries) == 1 {
			rf.debugPrint("replication", "set log %v", args.PrevLogIndex+1)
		} else {
			rf.debugPrint(
				"replication",
				"set logs %v through %v",
				args.PrevLogIndex+1,
				int(args.PrevLogIndex)+len(args.Entries),
			)
		}

	}

	if args.LeaderCommit > rf.commitIndex && rf.getLogTerm(rf.lastLogIndex()) == rf.currentTerm {
		rf.commitAndApply(min(args.LeaderCommit, rf.lastLogIndex()))
	}
	req.reply <- &reply
}

func (rf *Raft) handleVoteReply(r voteReply) {
	if !r.ok {
		return
	}
	if r.reply.Term > rf.currentTerm {
		rf.currentTerm = r.reply.Term
		rf.becomeFollower(r.reply.Term)
		return
	}
	if r.args.Term != rf.currentTerm {
		return
	}
	if rf.currentTerm >= r.reply.Term && r.reply.VoteGranted {
		rf.electionVotes++
		majority := (len(rf.peers) + 1) / 2
		if rf.electionVotes >= majority && rf.role != leader {
			rf.becomeLeader()
		}
	}
}

func (rf *Raft) handleVoteRequest(req voteRequest) {
	args := req.args
	reply := RequestVoteReply{}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// request came from old leader, reject
	if args.Term < rf.currentTerm {
		req.reply <- &reply
		return
	}
	// request came from new candidate
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	if rf.votedFor != -1 {
		req.reply <- &reply
		return
	}
	// grant vote if candidate's log is at least as up-to-date as own log
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.getLogTerm(lastLogIndex)
	if lastLogTerm > args.LastLogTerm {
		rf.debugPrint("election", "refused to vote for %v: log term [mine: %v theirs: %v]", args.CandidateId, lastLogTerm, args.LastLogTerm)
		req.reply <- &reply
		return
	}
	if lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex {
		rf.debugPrint("election", "refused to vote for %v: log count [mine: %v theirs %v]", args.CandidateId, lastLogIndex, args.LastLogIndex)
		req.reply <- &reply
		return
	}
	rf.debugPrint("election", "voted for %v in term %v", args.CandidateId, args.Term)
	rf.resetElectionTimer()
	rf.votedFor = args.CandidateId
	rf.persist()
	reply.VoteGranted = true
	req.reply <- &reply
}

func (rf *Raft) handleInstallRequest(req installRequest) {
	defer func() {
		req.reply <- InstallSnapshotReply{Term: rf.currentTerm}
	}()
	// request came from old leader, reject
	if req.args.Term < rf.currentTerm {
		return
	}
	// request came from new candidate
	if req.args.Term > rf.currentTerm {
		rf.becomeFollower(req.args.Term)
		return
	}
	rf.resetElectionTimer()
	lastIndex := req.args.LastIncludedIndex
	lastTerm := req.args.LastIncludedTerm
	// our snapshot is up to date
	if rf.snapshot.LastIndex >= lastIndex {
		return
	}
	rf.debugPrint("snapshot", "installing snapshot %v", lastIndex)
	// install snapshot
	rf.snapshot.LastIndex = lastIndex
	rf.snapshot.LastTerm = lastTerm
	rf.snapshot.Data = req.args.Data
	rf.commitIndex = lastIndex
	// clear logs covered by snapshot
	// if last log is found and term matches snapshot
	if lastIndex >= rf.lastLogIndex() && rf.getLogTerm(lastIndex) == lastTerm {
		// retain log entries following last log from snapshot
		rf.clearLogThrough(lastIndex)
	} else {
		// otherwise, discard entire log
		rf.log = []entry{{0, nil}}
	}
	// send snapshot to application
	rf.applyCh <- raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      req.args.Data,
		SnapshotIndex: int(lastIndex),
		SnapshotTerm:  lastTerm,
	}
}

func (rf *Raft) handleSnapshotRequest(req snapshotRequest) {
	if req.index < rf.snapshot.LastIndex {
		return
	}
	rf.debugPrint("snapshot", "creating snapshot for index %v", req.index)
	rf.snapshot.LastTerm = rf.getLogTerm(req.index)
	rf.clearLogThrough(req.index)
	rf.snapshot.LastIndex = req.index
	rf.snapshot.Data = req.snapshot
	rf.persist()
}

func (rf *Raft) becomeFollower(term int) {
	if rf.role != follower {
		rf.debugPrint("role", "became follower")
	}
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	rf.role = follower
}

func (rf *Raft) becomeCandidate() {
	rf.role = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.electionVotes = 1
	rf.debugPrint("role", "became candidate")
	rf.requestAllVotes()
}

func (rf *Raft) becomeLeader() {
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex()
		rf.matchIndex[i] = 0
		rf.appendLastSent[i] = time.Time{}
	}
	rf.role = leader
	rf.debugPrint("role", "became leader")
	rf.sendAllAppendRequests()
}

func (rf *Raft) sendAllAppendRequests() {
	rf.heartbeatTicker.Reset(heartbeatInterval)
	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		rf.sendOneAppendRequest(id)
	}
}

func (rf *Raft) sendOneAppendRequest(id int) {
	lastSent := rf.appendLastSent[id]
	if time.Since(lastSent) < heartbeatInterval {
		return
	}
	rf.appendLastSent[id] = time.Now()
	nextIndex := rf.nextIndex[id]
	var args AppendEntriesArgs

	if rf.snapshot.LastIndex > 0 && nextIndex <= rf.snapshot.LastIndex {
		rf.sendInstallSnapshot(id)
		args = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.snapshot.LastIndex,
			PrevLogTerm:  rf.snapshot.LastTerm,
			Entries:      []entry{},
			LeaderCommit: rf.commitIndex,
		}
		return
	} else {
		prevLogIndex := max(nextIndex-1, 0)
		prevLogTerm := rf.getLogTerm(prevLogIndex)
		args = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      rf.copyLogFrom(nextIndex),
			LeaderCommit: rf.commitIndex,
		}
	}
	rf.AppendEntriesRPC(id, args)
}

func (rf *Raft) sendInstallSnapshot(serverId int) {
	rf.debugPrint("replication", "sending %v snapshot %v", serverId, rf.snapshot.LastIndex)
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshot.LastIndex,
		LastIncludedTerm:  rf.snapshot.LastTerm,
		Data:              rf.snapshot.Data,
	}
	rf.InstallSnapshotRPC(serverId, args)
}

func (rf *Raft) requestAllVotes() {
	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		lastLogIndex := rf.lastLogIndex()
		lastLogTerm := rf.getLogTerm(lastLogIndex)
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		rf.RequestVoteRPC(id, args)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	req := stateRequest{reply: make(chan stateReply)}
	rf.stateRequests <- req
	state := <-req.reply

	return state.currentTerm, state.isLeader
}

func (rf *Raft) resetElectionTimer() {
	t := electionTimeoutBase + (rand.Int63() % electionTimeoutRandom)
	rf.electionTimer.Reset(time.Duration(t) * time.Millisecond)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshot.LastIndex)
	e.Encode(rf.snapshot.LastTerm)
	raftstate := w.Bytes()
	if rf.persister != nil {
		rf.persister.Save(raftstate, rf.snapshot.Data)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []entry
	var snapshotIndex logIndex
	var snapshotTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&snapshotTerm) != nil {
		panic("failed to read persisted data")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.commitIndex = snapshotIndex
		rf.snapshot.LastIndex = snapshotIndex
		rf.snapshot.LastTerm = snapshotTerm
		rf.snapshot.Data = snapshot
	}
}

func (rf *Raft) commitAndApply(newCommitIndex logIndex) {
	// skip dummy log at index 0
	startIndex := rf.commitIndex + 1
	if newCommitIndex < startIndex {
		return
	}
	rf.debugPrint("commit", "committing from %v until %v", startIndex, newCommitIndex)
	for i := startIndex; i <= newCommitIndex; i++ {
		entry, ok := rf.getLog(i)
		if !ok {
			rf.debugPrint("commit", "failed to apply log %v, not found", i)
			continue
		}
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			CommandIndex: int(i),
			Command:      entry.Command,
		}
	}
	rf.commitIndex = newCommitIndex
}

// Convert a `logIndex` to an int which can be used to index `rf.log`
func (rf *Raft) logIndex(index logIndex) int {
	return int(index - rf.snapshot.LastIndex)
}

// Index of the last log, including snapshot
func (rf *Raft) lastLogIndex() logIndex {
	return rf.snapshot.LastIndex + logIndex(len(rf.log)-1)
}

// Get a log entry, with an 'ok' bool signifying if an entry is found
func (rf *Raft) getLog(index logIndex) (entry, bool) {
	i := rf.logIndex(index)
	if i <= 0 || i >= len(rf.log) {
		return entry{}, false
	}
	return rf.log[i], true
}

// Get term for a log entry. Returns snapshot term if log is in snapshot
func (rf *Raft) getLogTerm(index logIndex) int {
	if index <= rf.snapshot.LastIndex {
		return rf.snapshot.LastTerm
	}
	return rf.log[rf.logIndex(index)].Term
}

// Find the index of the first log entry for `term`
// Returns -1 if there is no such entry
func (rf *Raft) findTermStartIndex(term int) logIndex {
	for i, log := range rf.log {
		if log.Term == term {
			return rf.snapshot.LastIndex + logIndex(i)
		}
	}
	return -1

}

// Delete all log entries after `index` exclusive
func (rf *Raft) clearLogAfter(index logIndex) {
	if index > rf.lastLogIndex() {
		return
	}
	if index <= rf.snapshot.LastIndex {
		rf.log = []entry{{0, nil}}
		return
	}
	rf.log = rf.log[:rf.logIndex(index)+1]
}

// Delete log entries from start until `index` inclusive
func (rf *Raft) clearLogThrough(index logIndex) {
	if index <= rf.snapshot.LastIndex || index > rf.lastLogIndex() {
		return
	}
	newLog := rf.log[rf.logIndex(index)+1:]
	for i, entry := range newLog {
		rf.log[i+1] = entry
	}
	rf.log = slices.Delete(rf.log, len(newLog), len(rf.log)-1)

}

// Create a copy of the log starting at `index`, inclusive until the end
func (rf *Raft) copyLogFrom(index logIndex) []entry {
	if len(rf.log) == 1 {
		return []entry{}
	}
	i := max(rf.logIndex(index), 1)
	return slices.Clone(rf.log[i:])
}

// Insert log entries starting at a given index
func (rf *Raft) setLogEntries(entries []entry, startIndex logIndex) {
	for i, entry := range entries {
		index := rf.logIndex(startIndex) + i
		if index < 1 {
			continue
		}
		if index >= len(rf.log) {
			rf.log = append(rf.log, entry)
		} else {
			if rf.log[index].Term != entry.Term {
				rf.debugPrint(
					"replication",
					"mid-append inconsistency %v leaderTerm: %v myTerm: %v",
					index,
					entry.Term,
					rf.log[index].Term,
				)
				rf.log = rf.log[:index+1]
			}
			rf.log[index] = entry
		}
	}
}

func applicationWorker(in chan raftapi.ApplyMsg, out chan raftapi.ApplyMsg) {
	q := make([]raftapi.ApplyMsg, 64)
	for in != nil || len(q) > 0 {
		var next raftapi.ApplyMsg
		var sendOut chan raftapi.ApplyMsg
		if len(q) > 0 {
			sendOut = out
			next = q[0]
		}
		select {
		case msg := <-in:
			q = append(q, msg)
		case sendOut <- next:
			q = q[1:]
		}
	}
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
	rf.log = []entry{{0, nil}}
	rf.nextIndex = make([]logIndex, len(rf.peers))
	rf.matchIndex = make([]logIndex, len(rf.peers))
	rf.appendLastSent = make([]time.Time, len(rf.peers))
	rf.applyCh = make(chan raftapi.ApplyMsg)
	rf.heartbeatTicker = time.NewTicker(heartbeatInterval)
	rf.electionTimer = time.NewTimer(1 * time.Second)
	rf.resetElectionTimer()

	rf.startRequests = make(chan startRequest)
	rf.snapshotRequests = make(chan snapshotRequest)
	rf.stateRequests = make(chan stateRequest)
	rf.appendRequests = make(chan appendRequest)
	rf.appendReplies = make(chan appendReply)
	rf.voteRequests = make(chan voteRequest)
	rf.voteReplies = make(chan voteReply)
	rf.installRequests = make(chan installRequest)
	rf.installReplies = make(chan installReply)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	rf.debugPrint("server", "server %v created", me)

	go rf.actorLoop()
	go applicationWorker(rf.applyCh, applyCh)

	return rf
}

// Comment out a topic to keep that category out of the debug output.
var debugTopics = map[string]bool{
	// "rpc": true,
	// "client":      true,
	"commit": true,
	// "election":    true,
	"replication":   true,
	"inconsistency": true,
	"role":          true,
	"server":        true,
	"snapshot":      true,
}

func (rf *Raft) debugPrint(topic string, format string, a ...any) {
	if os.Getenv("RAFT_DEBUG") != "true" || !debugTopics[topic] {
		return
	}
	t := time.Since(time.Now().Truncate(time.Hour)).Milliseconds()
	role := "follower "
	switch rf.role {
	case leader:
		role = "leader   "
	case candidate:
		role = "candidate"
	}
	lastLog, _ := rf.getLog(rf.lastLogIndex())
	part1 := fmt.Sprintf(
		"[%v] %v %v  term:%v snap:%v log:%v/%v commit:%v last:{%v %v}",
		t,
		role,
		rf.me,
		rf.currentTerm,
		rf.snapshot.LastIndex,
		len(rf.log)-1,
		int(rf.snapshot.LastIndex)+len(rf.log)-1,
		rf.commitIndex,
		lastLog.Term,
		lastLog.Command,
	)
	debugLen := 100
	spaces := strings.Repeat(" ", debugLen-len(part1))

	part2 := fmt.Sprintf(format, a...)
	tester.Annotate(
		"Server "+strconv.Itoa(rf.me),
		part2,
		role+part1,
	)
	fmt.Printf("%v %v %v\n", part1, spaces, part2)
}
