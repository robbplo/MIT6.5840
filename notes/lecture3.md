# GFS / Big storage
Storage interface is a very useful and general model for designing distributed systems
This is a paper describing a real design used in production

**Why is it hard?**
Aggregate/parallel performance is usually the goal -> sharding
Faults are inevitable with thousands of machines -> tolerance
Fault tolerance -> replication on the data
Replication -> inconsistency
Consistency -> lower performance

There is a tension between performance and consistency of data
Weaker consistency creates problems for the applications

## Strong consistency
Ideal strong consistency model behaves the same as a 'single machine' system
Requests see data that reflects all previous operations in order
Still requires some thought to get this right, for example with KV store example
If two clients write at the same time, there must be some rules to determine the order

## Bad replication design
Two servers in our KV store keeping a copy of the data, each of which should be identical
Writes go to both servers, reads go to any server.
At the same time: Client1 writes 1 to both servers, Client2 writes 2 to both servers
Now each server could have processed the requests in a different order
There is great difficulty in keeping the two servers in sync, and many different solutions
Each solution has different trade-offs, often affecting the application programmer

## GFS
Decades of research into distributed systems at this point, with very little production use
Google was one of the first to implement these ideas in the real world
Vast datasets that could not be stored on one disk, such as a scraped copy of the entire web

Goals:
- Big and fast
- Global, reusable for others at Google
- Sharding within large files
- Automatic failure recovery

Limitations:
- Single data center
- Internal use
- Tailored for large sequential reads/writes

GFS's weak consistency was a heretical idea to the academics, quite revolutionary
Could get away with a single master which recovers very quickly from failure

## Master Data
mapping of filename -> array of chunk handles / identifiers (disk)
mapping of chunk handles -> list of chunkservers
                            version number (disk)
                            primary
                            lease expiration

this is operated on in RAM, but also stored on disk
Logs and checkpoints are used to store metadata on disk
Mutations are stored in the append-only log, before the mutation is carried out
Checkpoints copy the entire state to disk, reducing the amount of logs to replay on startup

## Read sequence
1. Request from master to read file 'f' from offset 'o'
2. Master returns chunk handle, list of servers. Client caches
3. Client chooses a chunkserver and reads from it

## Writes (record append)
Append a sequence of bytes to a named files
Writes require a primary replica

Client requests a write from master
No primary exists
Find an up-to-date replica (matching version number)
Pick a primary by granting a chunk lease
Increment version, which written to each replica
The chunkserver remembers it is primary without asking the master for the lease duration

Primary picks offset, all replicas are told to write at the same offset
Secondary will respond with `ok` if they wrote the data
If all replied `ok` primary replies `success`, if not the primary replies `error`
So it's possible that different replicas end up with different file states on disk
Version number does not change in this situation, that only changes when lease is granted

If the client receives `error`, it will retry the write
The write is done again on all replicas, possibly resulting in duplicate data
Clients are expected to be able to handle duplicate records when reading
Because of this, replicas are also not guaranteed to have records in the same order
There can also be padding spaces in the file where no data is written

Applications can either:
accept replicas have padding and duplicates b
handle reconciliation by reading from all replicas
use only sequential write operations instead of concurrent record append

Labs 2 and 3 focus on building a strictly consistent system

## Summary
Highly successful system at the time, and BigTable and MapReduce were built on top of it
Most serious limitation was the single master. RAM requirements limited cluster size
CPU load also became too much with too many clients
The semantics were strange to deal with for some applications
Master failure required human intervention
