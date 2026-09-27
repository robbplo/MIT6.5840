# Paxos Made Simple
At the heart: *synod* consensus algorithm.
Paxos algorithm: consensus appplied to state machine approach for distributed systems

## The consensus algorithm
### The problem
Suppose a collection of processes that can propose values.
Conditions for consensus:
- Only proposed values can be chosen
- A single value is chosen
- A choice is never incorrectly broadcasted

Three roles: *proposers, acceptors, learners*
Single process may take more than one role

Use asynchronous non-Byzantine model
- Agents fail, restart, run at arbitrary speed.
- Some information must be persisted in agent upon restart
- Messages can be duplicated, lost, delayed, but not corrupted

### Choosing a value
Easy solution: single acceptor agent -> Single point of failure

Multiple acceptors
Proposer sends proposal to a set of acceptors.
If majority of acceptors accept the same proposal, the value is chosen.
This suggests requirement: acceptor takes first proposal
Many proposers may propose several values: no majority reached

Proposals are always taken and majority is required.
Therefore: acceptors must be allowed to accept more than one proposal
Proposal consists of: `(unique number, value)`
Each proposer sends different proposals, number is always unique
Then a value is chosen when a proposal has been accepted by a majority of acceptors.

Multiple proposals can be chosen, as long as they all have the same value.
If a proposal is chosen, every higher-numbered proposal issued has the same value.

P2c:
For any proposal `(n, v)` there is a majority of acceptors which either:
- None have accepted any proposal numbered `<n`
- `v` is the value of the highest-numbered accepted proposal

The algorithm for issuing proposals which follows:

1. *prepare request* Proposer chooses `n` and asks acceptors:
    - Not to accept lower number proposals
    - What is the highest `<n` proposal which was accepted, if any
2. *accept request* If proposer gets response from majority, proposal is issued
   Number is `n` and value is the highest-numbered proposal

Acceptor algorithm:
P1a: proposal with `n` can be accepted if not responded to *prepare* with `<n`
Acceptor must persist higher-numbered *accepted* and *prepared* request


Full algorithm in summary:
**Phase 1**
A proposer sends *prepare* to majority of acceptors with number `n`.

If an acceptor gets *prepare* with `n` greater than it has seen responds with:
- a promise not to accept lower-numbered proposals
- the highest-numbered proposal it has accepted, if any

**Phase 2**
If proposer gets majority response to *prepare*, *accept* is sent to them
The *accept* request contains the higher-numbered value from the *prepare* replies 

If acceptor receives *accept* request it accepts it, unless higher-numbered *prepare* was taken.

### Learning a chosen value
Easy solution: each acceptor tells all learners which value it accepts
this requires many network requests

Given non-Byzantine failures, learners can pass information to each other
Acceptance is sent to a distinguished learner, which informs other learners.
Distinguished learner is then a single point of failure
A set of distinguished learners could be used, each of which informs all learners

Due to message loss, a value can be chosen without reaching any learner
In this case, new proposal must be chosen to inform learners

### Progress
Two proposers can block each other by issuing *prepares* with increasing numbers
To guarantee progress: distinguished proposer will be the only one to send proposals
The numbers will always increase, so eventually it will reach a majority
Proposer election must use either randomness or real time

### Implementation
Network of processes: each playing role of proposer, acceptor, and learner
Leader is elected, which is the distinguished proposer and distinguished learner
Stable storage is used to maintain acceptor information in case of restarts
Proposers choose numbers from disjoint sets to prevent duplicates
Proposers remember in stable storage the highest number it used

## Implementing a state machine
Simple distributed system: one server with many clients
Server is deterministic state machine which performs commands from clients
This implementation fails if the central server fails
Therefore we use multiple servers each of which maintains the state machine
Given the same sequence of commands, each server will end with the same state

To guarantee the order of this sequence, implement Paxos algorithm for each command.
Each server plays all roles in every instance of Paxos
A single leader is elected for all instances
Clients send command to the leader, who decides the ordering of commands

For example:
Leader decides that a specific client command is the 135th command in the sequence.
It proposes that command for the 135th instance of the consensus algorithm.
After majority of acceptors have responded, a command can be chosen for this instance

After new leader is elected:
Also was a learner, so will know most of the commands that have been chosen
If any commands are missing from its knowledge, it will start proposals for them

## Question
Suppose that the acceptors are A, B, and C. A and B are also proposers. How does Paxos ensure that
the following sequence of events can't happen? What actually happens, and which value is ultimately
chosen?

- A sends prepare requests with proposal number 1, and gets positive responses from A, B, and C.
- A sends accept(1, "X") to A and C and gets positive responses from both. Because a majority
  accepted, A thinks that "X" has been chosen. However, A crashes before sending an accept to B.
- B sends prepare messages with proposal number 2, and gets positive responses from B and C.
- B sends accept(2, "Y") messages to B and C and gets positive responses from both, so B thinks that
  "Y" has been chosen.

## Answer
Once a value is chosen, every higher-numbered proposal must have that same value.
- A sends prepare requests with proposal number 1, and gets positive responses from A, B, and C.
- A sends accept(1, "X") to A and C and gets positive responses from both. Because a majority
  accepted, A thinks that "X" has been chosen. However, A crashes before sending an accept to B.
- B sends prepare messages with proposal number 2, and gets positive responses from B and C.
  The response from C says that (1, "X") was previously accepted.
- B finds that the highest-numbered accepted proposal has the value "X".
  B sends accept(2, "X") to B and C and gets positive responses from both, so "X" is chosen.

Before A can send the *accept*, it must receive positive response from a majority of acceptors.
Then, once it starts to send *accept* requests, if a majority respond with success, the value is
chosen. When B starts its own proposal, it may choose a different majority of acceptors to address.
If a value was chosen, this new majority is guaranteed to contain an acceptor which contains the
chosen value. Even if the value was not chosen, B will still use the highest-numbered accepted value
found in its set of acceptors. 
