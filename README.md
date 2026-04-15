## API 
look into `api.md` for supported operations

crux: it supports basic list and stream operations, handles set and geo
operation along with pubsub mode.

## Design

Initially, all the handlers were redirected by giant `if` block. Later, I
thought, it was much better to have a registry. 

For the registry, to work, all the handlers have to follow the same contract.  
`type CommandHandler func(server *Redis, cmd Command)`

The objective of this project is learning Redis by implementing a part of its
functionality. Although, redis is a single threaded and driven by a single event
loop, I just thought of using Golang's concurrency and used a single lock for
the entire program to prevent race condition while accessing the shared data
structure.


Currently, I am not concerned about the data structure for list, streams and
sets. I want to ensure their correctness and once that is guaranteed, I can
think of using optimal data structures suitable for the task in hand.

Also, this document is a later addition. Although, one has greater clarity for
the specs when later written, but this also prevents me from preserving my
thoughts and mental state while I was grappling with the problem. For eg: I
might right about replication in greater clarity but I lose the fact that it was
very difficult and unlinear process, I finally
arrived at the solution after multiple hit and trials and a improved mental
model of concurrency. I think I should also work on documenting my thought
process while building any projects. The learning process, mental state etc are
useful information to spot and improve on weaknesses or what I need right.

## Tests: todo
Most of the test was performed by simply using redis-client `redis-cli` and
observing if the behaviours were as expected. And, this project is as good as
codecrafters testing suite. This project passed the codecrafters challenge so
there is certain level of confidence, however, real production systems don't
rely on third party testing tools.

So, I need to write tests for all the methods. 

## Concurrency
This is a great project to learn about concurrent programming. With event loop,
everything becomes a lot easier since only one process is running at any
instance so you don't have to worry about race conditions if you have correctly
designed a event loop goroutine responsible for handling client commands. But,
with each client having its own goroutine spawned up, making sure data structure
is locked and only one process is able to mutate and read at a particular time
becomes important. To avoid complexity, a single global lock is used.

The responsibility of lock handling is delegated to each handler.

