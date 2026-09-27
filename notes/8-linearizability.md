# Linearizability (through section 3.1)

> Linearizability is a correctness condition for concurrent objects that exploits the semantics of
> abstract data types.
> It permits a high degree of concurrency, yet it permits programmers to specify and reason about
> concurrent objects using known techniques from the sequential domain 

## Introduction
Concurrent system: sequentual processes communicating through shared typed objects
Includes message-passing and shared-memory architectures
An objects operations can arrive in the wrong order, which must be accounted for

Concurrent computation is *linearizable* if it is equivalent to a sequential computation
Allows concurrent object to be treated as regular local object
*Local property*: system is linearizable if each object is linearizable
    unlike *sequential consistency* and *serializability*
    objects can be implemented independently
    runtime scheduling can be decentralized

*nonblocking* property: processes are never forced to wait for totally defined operations

## Motivation
Two obvious requirements for a correctness condition of concurrent objects:
    Each operation appears to "take effect" instantly
    Order of nonconcurrent operations is preserved

Example using FIFO queue, E=enqueue D=dequeue
`E(x) A` means process A enqueues item x

H1 is intuitively acceptable:
    B dequeues x, and A dequeues y
    the enqueues for x and y were concurrent, they *could* have occurred so that this is expected

H2 is not intuitively acceptable:
    x was enqueued before y, but y is dequeued first. Not FIFO

The objects intended semantics define what is acceptable and what is not
FIFO has different requirements than e.g. a stack

## Histories
Concurrent system consists of: multiple sequential processes communicating through shared objects
Every object has a *unique name* and a *type*
A *type* defines possible *values* and *operations* which can be performed

Formal modeling of such a system uses a *history*:
    finite sequence of operation *invocation* and *response* events  
    *subhistory*: subsequence of a history

Notation:
    invocation: `<x op(args*) A>`
    response:   `<x term(req*) A>`
        x: object name
        op: operation name
        term: termination condition (Ok)
        A process name

Response *matches* invocation if object names and process names agree

History is *sequential* if:
    First event is an invocation
    Each invocation (except the last) is followed by *matching* response
    Each response (except the last) is followed by *matching* invocation

History is *concurrent* if it is not *sequential*

`complete(H)` is the maximal subhistory where every invocation has a matching response

Process subhistory `H|P` is subsequence of events in `H` with process names `P`
Object subhistory `H|x` is same but for object `x`

Histories `H` and `H'` are *equivalent* if for every process `P`, `H|P = H'|P`
    Each process has the same individual order of events in both histories
    Order between the processes may be different
History `H` is *well-formed* if if each for every porcess `P`, `H|P` is *sequential*
    object subhistory **does not** need to be sequential

Operation `e` is an invocation `inv(e)` and the next matching response `res(e)`
    Notation: `[x inv/res A]`
    `e0` *lies within* `e1` if `inv(e1)` < `inv(e0)` and `res(e0)` < `res(e1)`

Set of histories `S` is *prefix-closed* if for any `H` in `S`, every prefix of `H` is in `S`
*Single-object* history is one where all events relate to one object
A *sequential specification* is a prefix-closed set of single-object sequential histories for an object
A sequential history `H` is *legal* if each object subhistory `H|x` belongs to the sequential
specification for `x`
Sequential history in this paper is summarized by the value which reflects it state ant the end

## Definition of Linearizability
History H induces irreflexive partial order `<H` on operations:
    `e0 <H e1` if `res(e0)` precedes `inv(e1)` in `H`
    `<H` captures real time precedence ordering of operations in `H`
    Operations unrelated by `<H` are *concurrent*
    If `H` is *sequential*, `<H` is a *total order*

History `H` is **linearizable** if it can be extended with response events to `H'` in which
    L1: `complete(H')` is equivalent to some legal sequential history `S`
    L2: `<H` is a subset of `<S`

`H` is extended to `H'` because a pending invocation can take effect before its response.
Focus on `complete(H')` removes pending invocations which do not affect the current state. 

## Question
With a linearizable key/value storage system, could two clients who issue `get()` requests for the
same key at the same time receive different values? Explain why not, or how it could occur.

## Answer
In a key-value store the 'object' is a single key/value pair, with operations `k set(value)` and
`k get()`. 

Consider the following history `H`
```
k set(x) A
k Ok() A
k set(y) C
k get() A
k get() B

```

And a possible extended history `H'`
```
k set(x) A
k Ok() A
k set(y) C
k get() A
k get() B
k Ok(x) A
k Ok() C
k Ok(y) B
```

Then consider the following sequential history `S`
```
k set(x) A
k Ok() A
k get() A
k Ok(x) A
k set(y) C
k Ok() C
k get() B
k Ok(y) B
```

`S` is legal because each `get` observes the most recent `set`, following the sequential
specification for a key-value store. 
`H'` is equivalent to `S` because each process subhistory is equal between them. Notice that
`complete(H') = H'`. This satisfies the condition L1.

`S` is sequential so `<S` is a total order. In `H` there is one `k set(x) A` finishing before any
other invocations, and no other responses, so that is the extent of the `<H` order. It is clear that
`<H` is a subset of `<S`, satisfying L2.

This shows that `H` is a linearizable history.
