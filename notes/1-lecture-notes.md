# Introduction

If you can possibly create a program on a single computer, do it!
Distributed systems should only be used if a single computer is not enough.

Why to use it
- Fault tolerance
- Parallelism
- Physical separation
- Security / network isolation

Difficulty comes from
- concurrency 
- network failures
- performance tuning

Still some problems for which there are no great solutions.
Nowadays most large sites are based on distributed systems.


## Course structure
- Lectures
- Papers per Lecture
- 2 exams
- labs
- optional project

Learn to read papers: *skipping irrelevant details*

Preparing for the exams: look at old exams.

## Infrastructure vs applications

Infrastructure
- Storage
- Communication
- Computation

Focus is on storage, easy to build and clear semantics
Attempt to create abstractions which make a distributed system look like a plain system

Implementation topics
- RPC
- Threads
- Concurrency control

Performance
- Scalability (more computers means more speed)
Scaling up servers is cheaper than optimizing your program

## Fault tolerance

A single computer can easily stay up for a year without any issues.
1000 computers at that rate -> 3 failures a day.
Failures can always happen in data centers with unplugged network cables of failing switches.

**Availability**: continuing to operate during a certain set of failures
**Recoverability**: system may stop operating, but repair itself once failure is over
Availability kind of implies recoverability

Recoverability must use non-volatile storage to be able to recover after a computer restarts. NV
storage tends to be slow, so writes must be minimized

**Replication**: identical copies of the system, which invariably go out of sync
Different replicas are ideally in very different locations from eachother, to reduce the possibility
that one failure takes out multiple replicas

## Concistency

Consider a distributed key-value store
Put(k, v)
Get(k) -> v

The semantics of these operations can change dramatically in a distributed system.
If we have two replicas, a Put may propagate to R1, but fail to propagate to R2.
The systems could remain out of sync, meaning that a Put operation does not guarantee updates for
all replicas.

Strongly consistent systems will always Get the most recent Put.
Weakly consistent makes no such guarantee. Stale reads may be returned from Get.

Weak consistency is still important because strong consistency has a very high performance cost,
where all replicas need to reach a consensus for Puts or Gets, depending on the system.

There are many different techniques on how to leverage a system with weak consistency while it still
being useful for applications.

## MapReduce

Originally built by Google, paper from 2004
They were doing computations on the entire web, such as sorting all of the webpages into an index
There was a real need to be able to run these compute jobs with up to 10tb of data
Each of their computations involved distributing the algorithm across thousands of commodity PCs.

MapReduce introduced a framework which abstracted the distribution across the cluster, while the
application programmer would write simple Map and Reduce functions. The map function runs on each
file and produces a list of key-value pairs. This is an obviously parallelizable workload. The map
functions write their output to a set of intermediate files.

Shuffle: collecting all the intermediate data and column-orienting it for the reduce step. Requires
every reduce worker to fetch a file from every map worker.

The intermediate files are collected from all machines, and based on a hash function keys are spread
across Reduce tasks. Those produce a single key/value pair.


