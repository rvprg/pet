# SUMI

This is a library that implements the distributed consensus algorithm Raft.

The Raft consensus algorithm is described in a brilliant Diego Ongaro's PhD thesis, 
*Consensus: Bridging Theory and Practice.* ([pdf](https://github.com/ongardie/dissertation/blob/master/book.pdf?raw=true))

This library implements all the core functionalities:
* Leader Election
* Log Replication
* Dynamic Membership Changes
* Log Compaction
