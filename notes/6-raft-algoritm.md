# Raft Consensus Algorithm (Figure 2)

*Excludes membership changes and log compaction.*

## State

**Persistent on all servers** (write to stable storage before responding to RPCs):
- `currentTerm` — latest term seen (0 on first boot, increases monotonically)
- `votedFor` — `candidateId` voted for in current term, or null
- `log[]` — entries, each with a command and the term the leader received it in (first index 1)

**Volatile on all servers:**
- `commitIndex` — highest entry known committed (init 0, monotonic)
- `lastApplied` — highest entry applied to the state machine (init 0, monotonic)

**Volatile on leaders** (reinit after election):
- `nextIndex[]` — per server, next entry to send (init leader last log index + 1)
- `matchIndex[]` — per server, highest entry known replicated (init 0, monotonic)

## AppendEntries RPC
Leader → followers, to replicate entries (§5.3) and as heartbeat (§5.2).

**Args:** `term`, `leaderId` (for client redirect), `prevLogIndex`, `prevLogTerm`, `entries[]` (empty for heartbeat; batching allowed), `leaderCommit`
**Results:** `term` (so the leader can update itself), `success` (follower had a matching entry at `prevLogIndex`/`prevLogTerm`)

**Receiver:**
1. Reply false if `term < currentTerm` (§5.1)
2. Reply false if no entry at `prevLogIndex` with term `prevLogTerm` (§5.3)
3. On conflict (same index, different term), delete that entry and all following (§5.3)
4. Append any new entries not already in the log
5. If `leaderCommit > commitIndex`, set `commitIndex = min(leaderCommit, index of last new entry)`

## RequestVote RPC
Candidates → all servers, to gather votes (§5.2).

**Args:** `term`, `candidateId`, `lastLogIndex`, `lastLogTerm` (§5.4)
**Results:** `term`, `voteGranted`

**Receiver:**
1. Reply false if `term < currentTerm` (§5.1)
2. If `votedFor` is null or `candidateId`, and the candidate's log is at least as up-to-date as the receiver's, grant the vote (§5.2, §5.4)

## Rules for Servers
*Triggered independently and repeatedly.*

**All servers:**
- If `commitIndex > lastApplied`: increment `lastApplied`, apply `log[lastApplied]` (§5.3)
- If any RPC request/response has term `T > currentTerm`: set `currentTerm = T`, become follower (§5.1)

**Followers (§5.2):**
- Respond to RPCs from candidates and leaders
- If election timeout elapses with no AppendEntries from the current leader and no vote granted: become candidate

**Candidates (§5.2):**
- On conversion, start an election: increment `currentTerm`, vote for self, reset election timer, send RequestVote to all others
- Majority of votes → become leader
- AppendEntries from a new leader → become follower
- Election timeout → start a new election

**Leaders:**
- On election: send empty AppendEntries (heartbeat) to each server; repeat when idle to prevent election timeouts (§5.2)
- On client command: append to local log, respond once applied to the state machine (§5.3)
- If last log index ≥ `nextIndex` for a follower: send AppendEntries starting at `nextIndex`
  - Success → update `nextIndex` and `matchIndex` (§5.3)
  - Failure from log inconsistency → decrement `nextIndex` and retry (§5.3)
- If some `N > commitIndex` has a majority of `matchIndex[i] ≥ N` and `log[N].term == currentTerm`: set `commitIndex = N` (§5.3, §5.4)
