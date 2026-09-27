# Google File System

Needs:
- Fault tolerance to support thousands of machines
- Huge files, much bigger than normal
- Append-only data and sequential reads

Methods:
- Automatic recovery from faults
- Redesign I/O operations and block sizes
- Specific design to optimize for appends and sequential reads
- Relaxed consistency model with atomic append

## Design overview

Assumptions
- Large cluster of commodity hardware, many failures
- Modest number of large files
- Mostly large streaming reads and small random reads
- Mostly large appending writes, rarely modifying
- Support uncommon operations but do not optimize for them
- Highly concurrent appends to one file must be efficient
- High sustained bandwidth > low latency

Implemented as familiar interface, but no standard API.
*snapshot* is a low-cost copy for a file or dir tree
*record append* is atomic append without additional locking

### Architecture
Single *master* and multiple *chunkservers* accessed by *clients*.
Files are divided into block-sized *chunks*.
Each chunk is replicated on multiple chunkservers.
Master maintains all metadata, lease management, garbage collection, migrations.
Master sends heartbeats to chunkservers.
Heartbeats will also give instructions and collect state from chunkservers
> Nice way to reduce network calls over separate state transfer calls

GFS client code in application communicates with master and chunkservers.
No caching of file data on client or chunkserver
Single master allows for simple design and global knowledge for chunk placement.

### Read example

- Client translates filename and byte offset into chunk index within file
- Client sends filename and chunk index to master
- Master replies with chunk handle and replica locations
- Client caches this metadata
- Client requests data from closest replica


### Chunk size
One of the key parameters, landed at 64MB
Much larger than typical FS blocks
Advantages
- Reduces interactions between client and master
- Reduce network overhead by keeping TCP connection open
- Reduce size of metadata, allowing in-memory storage

Disadvantages
- Small files are inefficiently stored
- Frequently accessed small files can create hotspots

### Metadata

Three major types: 
- File and chunk namespaces
- Mapping from files to chunks
- Location of chunk replicas

Mutations are also stored in operations logs on disk. (WAL?)
The log allows simple master updates without risking data losses
Chunk location is not persisted, chunkservers provide this information.

### Operation Log

The persistent record of metadata, and logical timeline of concurrent operations
Changes are only visible to clients once metadata is persisted to all replicas
Batches updates together before flushing
Creates checkpoints to minimize startup time

### Consistency model
Relaxed consistency model which remains simple to implement.
File namespace mutations are atomic and handled by the master.
Namespace locking guarantees atomicity, operation log defines total order

Quoted:
A file region is **consistent** if all clients will
always see the same data, regardless of which replicas they
read from. 
A region is **defined** after a file data mutation if it
is consistent and clients will see what the mutation writes in
its entirety.
> What is the difference between them?

Concurrent successful mutations leave region *undefined* but consistent:
Clients see the same data, but it may not reflect what any one mutation has written
> So data is consistent, but maybe not all mutations have been applied?

Failed mutation leaves a region **inconsistent**, different clients can read different data.

Clients cache chunk locations, if chunk is moved stale read is possible.
Next open purges chunk cache

### Implications of consistency model
Writers create checkpoints so that readers will know when to stop
Record Append creates padding and duplicate chunks.
Checksums and unique identifiers can be used to filter thosse chunks.


## System interactions and implementation
### Leases and mutation order
Master grants chunk lease to one *primary* replica, which picks mutation order.
For 60 seconds, the chunk is managed by the primary, which can be extended.
Process:
1. Client asks who is primary replica, and where secondaries are
2. Master replies and client caches data, until primary says it lost lease
3. Client pushes data to all replicas, where it is buffered
4. After write, client asks primary to write. Primary assigns serial order to mutations.
5. Primary forwards write to secondaries, with same order
6. Secondaries reply that they have completed write
7. Primary replies to client. Any errors are forwarded. Retries start at step 3 before full retry

Each write is to a single chunk. 

### Data flow
Decoupled from control flow, pushed linearly along chunkservers
Optimized based on network topology

### Atomic record appends
Normal write specifies where the data is written.
Record append, the master will assign data to an offset, returned to the client.
'at least once' delivery
Appends to chunk, if it would exceed max size, chunk is padded and operation must be retried
Retry if any replica fails -> duplicates are possible

### Snapshot
Quickly create a copy of a file or dir tree using copy-on-write
Mutations are paused by ejecting the primary replica
The metadata is copied, so that the snapshot points to the same files as the original.
When a client now wants to write to a chunk of the snapshot, a copy of the chunk is created
The copy is done on the same chunkserver where the chunk was.

## Master operation

### Namespace management and locking
Locks are applied to regions of the file namespace to enable concurrent operations
Each node in the namespace tree has an associated read-write lock.

### Replica placement
Chunk replicas are a balanced across different racks and networks.

### Creation, re-replication, rebalancing
When a chunk is created, master chooses initial replicas.
Balancing available disk space and rate of creations across servers
And attempting to balance across different racks

Re-replication occurs when replicas fall below threshold
Prioritized based on how far it is from goal, or if client is waiting for it

Rebalancing occurs periodically in order to improve load balancing

### Garbage collection
Deletion does not remove the file from storage immediately.
Instead renamed, and periodically scanned/deleted by master
If a chunk is scanned and found to have no references, the chunkserver deletes it
Storage is immediately reclaimed if the same delete occurs twice.

### Stale data detection
Replicas may become stale if chunkserver misses mutations during downtime.
Version number is increased for chunk when lease is granted
This is persisted across all replicas atomically
If a server was down, they will miss the update and not increase the number
This will be caught by the master which will use only the highest version
Stale replicas are considered by the client not to exist.

## Fault tolerance and diagnosis

### High availability
Fast recovery:
Startup times are in a matter of seconds.
Processed are designed to be killed and restarted.

Chunk replication:
Replication levels can be chosen per part of the namespace
Master clones existing replicas as needed to reach the level of replication
Considering move to parity or ereasure codes

Master replication:
Mutations are committed only after applying to all master replicas
One master is in charge of all mutations and background processes
Restart on failure is almost instant, and falls back to starting a new master

### Data integrity
Checksumming happens on each chunkserver to detect corruption
Checksums are stored on disk and in memory
On read, checksum is verified against the stored version
On mismatch, error is returned to client and failure reported to master

## Question
Describe a sequence of events that would result in a client reading stale data from the Google File System.

## Answer
1. Client A requests a read from chunk *C*.
2. The master gives the chunk handle and replicas *r1, r2*. Client A caches this
3. *r1* goes down
4. Client B mutates *C*. A new lease is granted, and version number incremented.
5. *r1* comes back up, with the old version number.
6. Client A reads chunk *C* from *r1* because of cached metadata, resulting in a stale read

