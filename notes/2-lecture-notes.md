# RPC and Threads
Why use Go? C++ was used before, worked fine
Go provides convenient features: good support for threads, synchronization, net/rpc
Garbage collection prevents a whole class of memory bugs
Threads + manual memory management is particularly difficult, to know when all threads stop using an
object. (Manual reference counting)
C++ compile errors are so complicated that you often just find the line number and try to figure out
what went wrong by reading the code.

## Threads

Main tool to manage concurrency
In distributed computing concurrency is important because often one process will need to manage many
other processes.
Goroutines in Go are analogous to threads in any other language

In a serial program there is a single address space, there is a single thread of execution. One
program counter, one stack, one set of registers.
In a threaded program there will be multiple separate threads in the address space. Separate stack,
program counter, registers. Each thread can also be executed in parallel if the CPU is capable.

In Go, the main program is also a goroutine.

## I/O concurrency
Maybe one thread will wait to read from the disk, while another thread runs tasks on the CPU or
waits for a network request.
In our case, we use threads to wait on network requests to other machines
It's the overlapping of progress from different activities.

## Parallelism
If you have multiple cores, threads allow your program to use multiple cores at the same time.
In Go, goroutines will run truly in parallel
> Is this not the case in other languages?

In this course, parallelism is not the focus. We are not optimizing for load, but for scalability.

## Periodicity
It is easy to make a thread that sleeps in a specific period to run some kind of checks.
Doing this in a serial program is quite annoying.
For example checking if your worker is still alive in MapReduce.
Overhead is not an issue if you will use a fixed amount of them.
Can be pushed too far, running a million threads would get quite expensive

### Questions

> What is difference between async and concurrent?

How can we write a program that runs on a single thread but can still have multiple states?

Async / event-driven programming.
Single thread/event loop which wait for an event which triggers processing.
The state of the source of that event is stored in a table, instead of having a separate stack.
This is limiting to parallelism, not great for high-core machines.
But if there are many concurrent threads required, such as a web server with a million requests,
event-driven may be more performant because you avoid thread overhead.
Event-driven loops could run on multiple cores as well. JS for example runs the event loop on one core.

> Process vs thread

Go program is a single UNIX process. The threads are inside of the process.
Processes can not read from each-others memory. Threads can share memory and synchronize.

> Does a context switch affect all threads?

Imagine single-core machine. Runs multiple processes.
The OS gives CPU slices of time to run each process.
The context switch is going from one process to another.

Threads are provided by the OS. Context switch will changes the threads that are executed.
Go will multiplex goroutines on threads to reduce overhead.

## Thread challenges
Each thread can share memory, such as a cache
But sharing memory between multiple thread can create problems

If you have a thread that increments an integer variable,
There is a possibility that another thread will read the variable at the same time.
If two threads run `n = n + 1` at the same time, they may both load the same value.
They both add 1, and store the new value. Two threads incremented, the result is only `+1`
This is referred to as a **race**.

> Is each ASM instruction atomic?

Some instructions are atomic, some aren't.
Store operations are extremely likely to be atomic if usize.
1 byte store depends on the processor, because it likely involves a load.
Increment is unlikely to be atomic, but there are atomic versions.

How to prevent such a race? You add **locks**
A lock essentially makes a sequence of operations atomic
Lock is not connected to a value. The programmer ensures that the lock is used properly

> Should locks be private in a data structure?

Reasonable strategy to have the locking happen inside the methods.
Breaks down if the data is never shared, lock is unnecessary.
If two data structures use each-other, there is a risk of deadlocks.
Usual solutions for deadlocks require locks to be lifted into the calling code.

## Coordination
Different threads often do not know of each other.
In some cases they will interact with each-other and wait for tasks to finish.
In Go this is often done with channels. 
Also possible with condition variables sync.Cond or sync.WaitGroup

## Deadlock
T1 waits for T2 to release a lock. However, T2 may be waiting on a lock that T1 holds.
When two threads are waiting on eachother to continue, you get a deadlock.

## Webcrawler example

3 solutions in different styles.

Crawler program: fetch a page and fetch each url within the page.
Url pages are a cyclic graph, so crawler must remember all seen pages.
Imposes a tree structure for the cyclic graph of the web.
Great problem for concurrency because pages load slowly.
We want to be able to max out the network capacity.
The final challenge is to know when the crawl is finished.
For some solutions that can be the hardest part.

## Serial crawler
Essentially a DFS over the web graph.
One interesting thing is a `fetched` map to remember which pages are fetched.
Single table can store which pages are fetched.

## Concurrent crawler
Uses shared state and a mutex as a recursive function
Keeps bookkeeping to know if all crawls are finished.
Table of urls is still there, but shared between threads.
Mutex locks only around the read/write on the shared table.
Minimizing the amount of work inside the lock will reduce contention

WaitGroup is used to check if all newly found urls are fetched.
Before starting the goroutine, the WaitGroup is incremented.
Inside the goroutine, after the recursive call, the WaitGroup is decremented.
WaitGroup.Wait() will return only after the WaitGroup arrives at 0.
WaitGroup is a counting semaphore.

> What if a subroutine fails and does not decrement WaitGroup 

That is possible and would be a problem. Solution for that would be to add a defer to the call.


