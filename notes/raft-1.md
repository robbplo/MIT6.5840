# In Search of an Understandable Consensus Algorithm (2014)
Goal: consensus algorithm equivalent to Paxos but more understandable
Separate *leader election*, *log replication* and *safety*

## Introduction
Paxos dominated the scene until this point
Hard to understand, need complex changes to implement in real systems
Should be obvious why the algorithm works.

Similar to *Viewstamped Replication*
Novel features:
**Strong leader** is the only one to accept log entries.
Simplifies log replication
**Leader election** using randomized timers for simple conflict resolution
**Membership changes** using *joint consensus* with overlapping majorities
Cluster can continue operating while configuration changes

## Replicated state machines
State machines on a collection of servers compute copies of the same state
Can continue to operate if some servers are down
Used e.g. for leader election and config replication in GFS
Log of commands is replicated on each server and executed in the same order
Consensus algorithm is used to keep logs consistent

Requirements:
- Ensure **safety** under all non-Byzantine conditions
- Available as long as majority of servers is operational
- Do not depend on timing of logs
- Command can be processed when majority of servers has accepted


## What's wrong with Paxos?
Paxos defines protocol for choosing a single value: *single-decree Paxos*
For real system, many instances of this protocol must be combined
Together they will form a log: *multi-Paxos*

Very difficult to understand (IMO, even the simple version was dense)
Simplified versions exist, focusing on single-decree
Single-decree is dense: divided into two stages without intuitive explanation
Difficult to see why the single-decree protocol works

Paxos does not provide foundation for building practical systems
No agreed algorithm for multi-Paxos, and not defined by Lamport
Collection of logs is required, choosing then combining adds complexity
Symmetric peer-to-peer model is not practical in real systems
Coordination by a leader is simpler and faster
Real systems start with Paxos, then evolve it as difficulties arise

## Designing for understandability
Raft was created with these goals:
- Provide practical foundation for building sustems
- Be safe under all conditions
- Be available under typical conditions
- Be efficient for common operations
- (most difficult) Be easy to understand for a large audience

Techniques to attempt an understandable design:
- Divide problems into separate pieces where possible
- Simplify the state space, reducing possible set of states
    - For example, no holes in logs are allowed
- Use randomization to simplify leader election

## The Raft consensus algorithm
Raft manages a replicated log, as in for RSM
First elects *leader*, who has full responsibility for the log
Leader accepts new entries, replicates them, decides when its safe to apply them
Leader can decide the location of new entries by itself
If the leader fails or is disconnected, a new leader is elected

Raft splits consensus into three separate parths:
**Leader election**: a new leader is chosen when existing fails
**Log replication**: Leader accepts logs from clients and replicates in cluster
**State Machine Safety**: Logs may only be applied if all servers will do the same

## Basics
Typical to have 5 servers: can tolerate 2 failures
Server is either: *leader, follower, candidate*
Followers issue no requests to other servers
Leaders accepts client requests. If follower is connected to, should redirect
Candidate is used for election.

Time divides into *terms*, consecutive increasing integer
Start of term is *election*, one or more candidates attempt to become the leader
Split vote: term ends and reelection occurs
Terms act as logical clock, reject stale requests
Leader or candidate reverts to follower if newer term is discovered

Two RPCs: `RequestVote` and `AppendEntries`
Optional RPC for snapshots
Calls issued in parallel and retried if timed out

## Leader election
Servers begin as followers, will remain so as long as they receive RPCs from leader
After no heartbeat over *election timeout*, term ends and election begins
Follower increments term and becomes candidate, requests votes from all other servers

Candidate wins if majority votes for it in the same term
Servers has one vote per term, votes for first request 
If candidate receive `AppendEntries` from another server, only accepted if same or greater term
Split vote: time out and re-elect
Election timeout is randomized to try to avoid split votes

## Log replication
Leader accepts new commands, and sends `AppendEntries` to followers, indefinite retries
After safe replication, command is applied to state machine and response to client -> *commit*
Log entries have an index, the term number, and the command for the state machine
Committed entries will eventually be execeuted by all servers.

A *commit* occurs when majority of servers has replicated a log entry
Includes all preceding entries, also if they came from another leader
Highest committed index is in state, also sent in `AppendEntries`

Safety guarantee: *Log Matching*
- Two entries in different logs with the same index and term store the same command
    - Leaders creates one entry with a given index in a given term
- Two entries in different logs with the same index and term are preceded by identical entries
    - Guaranteed by `AppendEntries`
    - leaders include last index and term, follower rejects new entries if last one is not present

Log inconsistency occurs when leaders crashes, compounding with repeated crashes 
Possible inconsistency upon a new election:
a. Follower misses committed entries
b. Follower has extra uncommited entries
c. Combination of a. and b.
These can span multiple terms
Leader's logs will overwrite conflicting entries
Leader must find where logs diverge, remove entries after that from follower
`AppendEntries` is rejected by follower -> leader tries again, with log index -1
Eventually reaches the latest consistent point, follower accepts -> successful replication
Logs automatically converge toward consistency
*Can be optimized*

## Safety
Final step: restrict which servers can be elected to ensure consistent logs
**Leaders Completeness**: committed log entry appear in leaders logs of all higher-numbered terms
Leader must eventually store all committed log entries
Election can only be won if candidate has all committed log entries
`RequestVote` includes info about candidate's log, votes rejects if it has newer committed logs
Log containing the highest-numbered term is more up-to-date

When committing entry from a previous term, majority replication is not enough to ensure safety
A new leader can only commit using entries from its own term, also committing prior entries

At this point each property of the Raft algorithm can be proven

## Timing and availability
Safety must not depend on timing, but availability always depends on timing
Timing requirement:
Parallel request and response to all other servers must be much faster than election timeout
Election timeout must be much lower than average time between failures for single server

## Question 
Suppose we have the scenario shown in the Raft paper's Figure 7: a cluster of seven servers, with
the log contents shown. The first server crashes (the one at the top of the figure), and cannot be
contacted. A leader election ensues. For each of the servers marked (a), (d), and (f), could that
server be elected? If yes, which servers would vote for it? If no, what specific Raft mechanism(s)
would prevent it from being elected?


## Answer 
Server (a) could be elected. The first server's entry at index 10 was not committed, so (a) has the
latest committed log state. The only server that would not vote for it is (d), because it has log
entries with a higher term, in which case `RequestVote` is rejected.

Server (d) could be elected. It seems to have had another term as leader, so it has the highest term
number of all servers. Every server would vote for (d).

Server (f) could not be elected. No server would vote for it, because every other server has log
entries from terms higher than what (f) has. The **Leader Completeness** property states that
committed log entries for one term must be present in the logs of leaders for all later terms.
Electing (f) would violate this property.
