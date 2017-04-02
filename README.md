# TableDB
table database with replication

## NPL Raft Status
the NPL Raft implementation is feature complete and usable now! :)

The implementation is basically a port of [jraft](https://github.com/datatechnology/jraft) to NPL

Raft consensus implementation in NPL

> The core algorithm is implemented based on the TLA+ spec, whose safety is proven, and liveness is highly depended on the pseudo random number sequence, which could be fine if different servers in the same cluster are generating random numbers with different seeds.

### Supported Features,
- [x] Core Algorithm(Leader election,Log replication), safety is proven
- [x] Configuration Change Support, add or remove servers one by one without limitation
- [x] Client Request Support
- [x] **Urgent commit**, see below
- [x] log compaction 

> Urgent Commit, is a new feature introduced by this implementation, which enables the leader asks all other servers to commit one or more logs if commit index is advanced. With Urgent Commit, the system's performance is highly improved and the heartbeat interval could be increased to seconds,depends on how long your application can abide when a leader goes down, usually, one or two seconds is fine. 

### About this implementation
> it's always safer to implement such kind of algorithm based on Math description other than natural languge description.
> there should be an auto conversion from TLA+ to programming languages, even they are talking things in different ways, but they are identical

you can test with the `cd setup && setup.bat`. It will start 3 Raft nodes, and 1 client. the client will send commands to the Raft cluster. All 3 Raft nodes will recv the same commands, either succeed or not the client's callback will be called. The 3 Raft nodes is *fault tolerate* and *highly available*. You can stop the cluster and client with `stopNPL.bat`.

welcome for more feedbacks.:)
