# Fault tolerance, Raft 1

Discussed systems have had single master
Single point of failure
When multiple masters -> split brain can occur

## Split brain
Consider replicated system where client talks to all servers
If all servers must respond OK, we have less fault tolerance than single server
If instead client talks to one server: network partitions will cause inconsistency
Split brain problem: two or more servers start to act independently and create conflicts

Solutions?
- Build a network that cannot fail (given enough money, practically achievable
- Human sorts out conflicts and errors

Main automated solution: majority vote
Works because: any two majorities overlap in at least 1 server
More than half of servers must agree
As long as majority of servers is in the partition, progress can be made

Raft it correct because:
Each majority will contain at least 1 server from the previous leader's majority
At least 1 server in the new majority will contain the committed logs from the previous term

## Raft
Client talks to application layer as normal
Application layer sends request to raft, waiting for the request to be committed
Only after it is committed, Raft yields to the application and a response can be sent 
Once the commit happens, each server applies the command to their own state machine
When a leader fails, any follower can become the new leader
Therefore each needs to keep a copy of the log

Start function sends command to Raft which returns immdediately
applyCh sends committed commands to client
Start function gets returned index for command and term
applyCh gets command and index
These are used by the tester to validate correctness

Logs may not be identical, at least temporarily
If an entry is partially replicated but not committed, it will eventually be deleted

## Leader election
Leader is not required to create consensus system
But in practice it tends to perform better, and simple to understand
Followers don't need to know who leader is, just the term number

Election timer should be at least a couple times the heartbeat interval
