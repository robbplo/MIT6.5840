# Patterns and Hints for concurrency in Go
Concurrency is *dealing with* many things at once
Parallelism is *doing* many things at once

Goroutines for state: parser example
Not really understanding these examples without speakers explanation.

## PubSub server
Refer to [mutex](./go-patterns/pubsub-mutex.go) and [channel](./go-patterns/pubsub-channel.go) code.

General idea is that multiple goroutines may access the public methods of the `Server` struct.
In the mutex version, those methods each refer to a `sub` map stored in the `Server` struct.
Mutex is used to synchronize access from these different public methods.

In the channel version, `Server` now contains 3 channels, one for each public method. The `sub` map is gone.
the `Server` initializer also starts an infinitely looping goroutine calling `loop()`
The `loop` method contains a local variable, which replaces the `sub` map on the `Server` struct.
Inside the method is a `select` statement, receiving on each of `Server`'s channels.
Each case in the select can safely access the `sub` map, because `select` will run one case at atime
The public methods now send requests and receive feedback from channels

Wondering what the performance effects would be of such changes.
Russ Cox does not seem to worry about that at all, but rather about code clarity.
> Hint: Use additional goroutines to hold additional code state.
> Hint: Convert mutexes into goroutines when it makes programs clearer

The channel version really reminds me of `GenServer` in Elixir/Erlang.
The `loop` method is the server, with a `select` statement containing all cast/call definitions
Public methods are the client, which may be called from any amount of goroutines.
A `cast` is implemented like `Publish`: send on a channel and return
A `call` is implemented like `Subscribe`: send on input channel and receive on an `ok` channel

Main shortcoming now is that the Go code will block in `Publish` if no goroutine is receiving on the
channel. Queueing events would alleviate the issue. Russ warns againt unbounded queueing, and
proceeds to implement an unbounded queue in the following examples.

> Hint: Use goroutines to let independent concerns run independently.

The unbounded queue version reveals some interesting behaviours. 
The first iteration simply tries to send the first item in the queue, but crashes if empty.
Second version creates another channel variable, which is only assigned to if the queue is not empty
When a channel is not assigned to, its value is `nil`.

Sending to or receiving from a `nil` channel with the normal `<-` syntax will block forever. 
But this is used inside of a select statement with a receive on the `in` channel.
A clever way to modify the channels which a select statement waits on.

You can imagine a much simpler version with a buffered channel like `make(chan Event, 10)`
It could reduce some of the blocking from `Publish`, but not eliminate it.
In order to eliminate the blocking you have the unbounded queue version, or a version which drops
messages.

## Work scheduler
Not a very clear example for me. sync.WaitGroup seems more appropriate.
Interesting to keep to simple primitives for something like this though.

Do love this concept, reminiscent of BEAM actor model
> Hint: Don’t communicate by sharing memory. Share memory by communicating.

> Hint: Make sure you know why and when each goroutine will exit.

So be explicit about exit conditions for goroutines, do not leave them blocking indefinitely

Really missed the explanations for this one

## Replicated service client
Back to a mutex with this hint, makes a lot of sense
> Hint: Use a mutex if that is the clearest way to write the code

The last thing i expected to see was the `result` struct definition inside of the `Call` function.
Initially it seems heretical to do something like that, but i can think of some justifications:
- The struct is not used by any other function, so why should it pollute the package namespace?
- Because its local to `Call` there is no ambiguity about what the result is for. If your package
  has 4 different kinds of calls, they could each define their own result struct as just `result`
- It's how you might use a tuple in other languages. We are just trying to send two values on the
  same channel.


> Hint: Use a goto if that is the clearest way to write the code.

The other last thing i expected to see was a `goto` statement, let alone a recommendation to use them.
I have not worked much on low-level software but `goto` seems like something people avoid like the plague.
I can see the use here, whenever the function returns, the preferred server should be updated.
Just duplicating the update to prefer with 2 returns IMO would be clearer

## Protocol multiplexer
Just a nice example using goroutines, channels, mutexes all together. No different versions here

## Takeaways
Some great examples of channel usage in intuitive or clever ways.
In the PubSub example with channels, the similarity with GenServer/Actor model was eye opening.
"Share memory by communicating" with channels provides a simple mental model for concurency.

