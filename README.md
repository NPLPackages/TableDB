# TableDB
table database with replication

## NPL Raft Status
the NPL Raft implementation is feature complete and usable now! :)

The implementation is basically a port from [jraft](https://github.com/datatechnology/jraft) to NPL

### Raft consensus implementation in NPL

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

#### Threading model
  Now the implementation is Single thread. To improve performance, we can put the I/O operation into one thread(eg. the commit）
  
#### Logic
  The Core Raft algorithm logic is in RaftServer, its implementation is straight forward
  
### Test

#### Start a 3 Raft Nodes cluster
 `cd setup && setup.bat` will start 3 Raft nodes. The 3 Node will automatically elect a leader. we can stop one node(whether it is leader or not) and the cluster still function correctly. Raft will tolerate `N/2 + 1` nodes failue, where N is the total nodes in the cluster.

#### Send Commands to the Cluster
`setup.bat client appendEntries` will start 1 client node, and it will send commands to the cluster and automaticlly retry in a backoff way if the command not succeed. All 3 Raft nodes will recv the same commands, either succeed or not the client's callback will be called. One known issue is the command may commit twice in the cluster due to the retry. A not so good we to fix this is disable the retry.
 
#### Add a server to the Cluster
`addsrv 5` will start a node which id is 5. To add the server to the cluster you can execute `setup.bat client addServer 5`. You may need to add twice(execute `setup.bat client addServer 5`) because the initial connect caused by `NPL.activate()`. The cluster will automatically sync the logs previous commited to the new add server. And if you start a client to send Commands the new server will also recv these commands.

#### Remove a server in the Cluster
`setup.bat client removeServer 5` will remove the node 5 in the cluster. Now you can not request to remove leader.

Note: every time you start a new client(whether Send Commands, Add server or Remove server), you should stop the previous client.


The Raft cluster is *fault tolerate* and *highly available*. You can stop the cluster and client with `stopNPL.bat`.

welcome for more feedbacks.:)
