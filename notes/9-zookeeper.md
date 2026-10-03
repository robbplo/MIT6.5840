# ZooKeeper

Service for coordinating processes of distributed applications
High performance service implementation
Per client FIFO execution guarantee
Linearizable writes

## Introduction
One approach to coordination: services for each separate need
    SQS for queue, Raft for leader election
    Each service implements some set of primitives
Zookeeper allows developers to implement their own primitives instead
    **coordination kernel**
    Enables multiple forms of coordination
Avoiding *blocking primitives* such as locks
    Slow clients will affect fast clients
    Failure detection requires complex implementation
Instead using *wait-free* data objects
    API resembles a file system
Coordination still needs guarantees
    FIFO order of all requests per client
    Linearizable writes
Zookeeper service manages all coordination for application
    Implementation uses pipelines architecture, naturally FIFO
    Client can have multiple outstanding requests, async
Zab: leader-based atomic broadcast protocol
Heavy on client-side caching
    Pings clients when a cached value is updated

## ZooKeeper service
Client library manages network connections and exposes ZooKeeper API
*client*: user of ZooKeeper service
*server*: process providing ZooKeeper service
*znode*: in-memory data node of ZooKeeper's *data tree*
*session*: client connection to ZooKeeper which includes a seesion handle

znodes are organized in a filesystem-like hierarchical namespace
    *regular znode*: explicitly created and deleted by clients
    *ephemeral znode*: created by clients, but can be automatically deleted by the system
*sequential flag*: appends name with increasing counter
    value of counter increases monotonically under the same parent node 
*watch*: zookeeper will notify the client if this znode changes

Modeled after file system but not intended for data storage
Sessions expire after some time without any requests being sent

## Client API
```
create(path, data, flags)
delete(path, version)
exists(path, watch)
getData(path, watch)
setData(path, data, version)
getChildren(path, watch)
sync(path)
```
Sync and async versions for each method
Version number is for conditional update, -1 will always write

## Guarantees
**Linearizable writes** and **FIFO client order**

*A-linearizability*: allow client to have multiple outstanding operations
    They are completed in FIFO order
    Also satisfies original definition for linearizability
    Only updates are a-linearizable
    Reads are local to each replica (linear scaling of reads)

Combination of properties which allows intuitive implementations
Zookeeper can make progress while a majority of servers is active

## Examples
- Configuration management
- Group membership
- Simple Lock
- Lock with watch to avoid herd effect
- R/W lock

## Zookeeper Applications

### Fetching service
Part of Yahoo crawler
Master process commanding fetching processes
ZK helps with:
    master failure recovery
    availability guarantees
By implementing:
    configuration metadata
    leader election

### Katta
Distributed indexer
main/worker configuration
ZK:
    tracks status of worker nodes
    leader election
    configuration management

## ZooKeeper implementation
Data is replicated on each providing server

```
request -> [request processor]
[request processor] -> [atomic broadcast]
[atomic broadcast] -> [replicated DB]

read request -> [replicated DB]

[replicated DB] -> response
```

DB is in-memory, entire tree
Writes are also logged to disk, WAL
Periodic snapshots of in-memory DB

All servers handle client requests
Write requests are forwarded to leader by client

### Request processor
Servers never diverge
Idempotent transactions, future state calculated before commit
Transaction fails: error transaction is generated

### Atomic broadcast
Update requests are broadcasted using Zab: atomic broadcast protocol
Majority quorum to make decision
All changes from previous leader are delivered before leader can broadcast
Changes are delivered in the order they are sent
Zab leader = zookeeper leader
Zab proposal log = WAL for memory DB

### Replicated database
Snapshots speed up crash recovery
*Fuzzy snapshots*: taking snapshot without lock
    DFS the tree and write to disk
    May diverge from valid state
    State changes are idempotent, replay events to restore

### Client-server interactions
Writes are sequential, ensuring notification order
Read requests are aware of most recent write request
    Allows processing locally on server
    Stale reads are possible, but can use `sync` to avoid them
Reads are tagged with *zxid* corresponding to last seen transaction for the server
    If a client has seen later *zxid* than the server it tries to connect to, server refuses
    Part of durability guarantee

## Question
One use of Zookeeper is as a fault-tolerant lock service (see the section "Simple locks" on page 6).
Why isn't possible for two clients to acquire the same lock? In particular, how does Zookeeper
decide if a client has failed and it can give the client's locks to other clients?

## Answer
Two clients cannot acquire the same lock because of multiple mechanisms. A lock is acquired by
creating a znode in a particular path. If the znode exists, another client will not be able to
create it because writes are linearizable. Therefore if the lock is acquired, the second client is
guaranteed to be unable to create it, and can watch it to attempt to acquire it after it is released.

Zookeeper knows that a client has failed when a session has not exchanged any messages for a certain
amount of time. The session ends at that point. The znode for the lock uses the EPHEMERAL flag,
which means that the znode will be deleted as soon as the session it was created in ends. This means
that the lock is released automatically when the client fails.
