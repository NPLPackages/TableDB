# TableDB
table database with replication

## Note
the project is still under development, for much detail see the [wiki](https://github.com/NPLPackages/TableDB/wiki) and the [develop branch](https://github.com/NPLPackages/TableDB/tree/develop)

## Usable NOW
the NPL Raft implementation is basically usable now! :)

the status is as follows
  * [x] Leader election
  * [x] Log replication
  * [x] Client interaction
  * [ ] Cluster membership changes
  * [ ] Log compaction

you can test with the `cd setup && setup.bat`. It will start 3 Raft nodes, and 1 client. the client will send commands to the Raft cluster. All 3 Raft nodes will recv the same commands, either succeed or not the client's callback will be called. The 3 Raft nodes is *fault tolerate* and *highly available*. You can stop the cluster and client with `stopNPL.bat`.

welcome for more feedbacks.:)
