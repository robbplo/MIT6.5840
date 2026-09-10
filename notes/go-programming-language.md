# The Go Programming Language and Environment
Garbage collected, statically compiled systems language
To know why it really took off, lets look atthe environment
Examine design decisions most responsible for Go's success

## Origins
Go came from building distributed systems at google with many engineers
4k engineers using a single multi-language codebase
Many dependencies, fixing one bug may cause anothers
C++ library compilation was brutal, building Bazel was not enough

Google ran massive systems already at the time.
Still struggling to take advantage of multicore systems
Opportunity at language level: simple primitives for threads and parallel garbage collector
Go was created to meet these challenges
Popular because entire industry is facing these challenges today

## Packages
Go program is made up of packages
Each import reads only a single file
`fmt` references `io` package, but importing `fmt` does not recursively import `io`
Compiled metadata of `fmt` contains all necessary information to know about `fmt`
Avoids repeated recursive compilations of package dependencies -> faster builds
Import cycles are not allowed, which means that builds can be split up
Packages are identified by url-like paths

## Types
Basic set of scalar types are provided
Also pointers, fixed-size arrays, structs, C style
More specific: strings, slices (dynamic array), map (hash table)
Most programs do not use other container types
Methods can be bound to any type, even basic types
No type hierarchy/inheritance, but composition
Object oriented polymorphism through interfaces
Interfaces do not need to be explicitly implemented
Accidentally implementing an interface is possible in theory, but unlikely in practice

## Concurrency
Threads were difficult to use in most languages
Go contains goroutines, sharing an address space and being multiplexed on OS threads
Goroutines start with a few KB of resizable stack, inexpensive to create many
`go` arguments are evaluated in original goroutine, copied to new goroutines first stack frame
Under the hood uses Linux `epoll` or similar handle concurrent I/O

Channels are provided for coordination between goroutines
Unidirectional pipe of limited size
These ideas are adapted from Hoare’s "Communicating Sequential Processes"
Article shows a very simple example for a thread pool webserver using channels
Sending data on a channel passes ownership from sender to receiver
Other synchronization primitves are available, but channels are often better
Garbage collector makes concurrent API design much simpler
TSAN provides dynamic race detector

## Security and Safety
Go removes undefined behaviors pervasive in C and C++
No integer type coercion, runtime exceptions on null pointer deref and out-of-bounds array access
No dangling stack pointers: variable that can outlive stack frame will be moved to the heap
Garbage collector prevents use-after-free bugs
Automatically assigning `0` values to allocated memory
Integers can still overflow though
Typesystem violations and pointer arithmetic provided in `unsafe` package
Also ships with cryptographic libraries

## Completeness
Provides enough to be useful out of the box, but not too much
No competing implementations for strings or hashmaps in package ecosystem
Concurrency in the core provides a uniform approach for all users
Stdlib provides production ready HTTPS client and server
Common interfaces in `io` and `http` packages allow libraries to interoperate

## Consistency
Go was meant to behave the same in all implementations and execution contexts
One exception is in maps, due to iteration depending on the hash function
Therefore map iteration was defined to be non-deterministic
Each map has a different seed and starts iteration at a random offset
All that to prevent code from accidentally depending on implementation details

Performance is consistent due to traditional compilation instead of JIT
Makes Go effective for long-lived or short-lived programs
Original prototype used stop-the-world GC which introduced a lot of tail latency

Addendum: https://blog.jetbrains.com/go/2026/07/20/escape-analysis/
## Escape analysis
Compiler chooses if variable is allocated on stack or heap
If stack allocation can be unsafe, heap is chosen. E.g.:
- Function which returns a pointer to a local variable
- Closure which passes variable to a goroutine
- When value is stored in a container which outlives function


## Question
What do you like best about Go? Why?
Would you want to change anything in the language? If so, what and why? 

## Answer
I love the idea of channels, reasoning of message passing as synchronization.
Not always intuitive, the parser example did not seem more readable with channels
To me, channels are much easier to reason about than mutexes

I do really miss functional map/filter/reduce in Go.
Not implemented to ensure a 'canonical' solution to most problems, which is valid
For example, writing a for loop to filter a slice
```go
nums := int[]{1,2,3,4,5}
var even int[]

for _, num := range nums {
    if num % 2 == 0 {
        even = append(even, num)
    }
}
```

This feels the identity of Go however: simple and boring.
No fancy syntactic sugar, just focusing on imperative logic.
Maybe that's something i need to get used to.
Another pattern, which has the same reasoning, i would absolutely change:

```go
bytes, err := io.ReadAll(r)
if err != nil {
    return err
}
```

Errors as values is great. Clear where errors can occur, must be explicitly handled.
Returning the error to the caller is a very common pattern.
I think Zig got it right here. Errors are values, but handled separately from return values
That allows a syntax like `try f()`, which returns the error from f if f returned an error.






