# TableDB
table database with replication

## NPL Raft
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
  Now the implementation uses 2 threads. 
  1. main thread.
  2. create snapshot thread, which used when create a snapshot for the state machine.

#### Logic
  The Core Raft algorithm logic is in RaftServer, whose implementation is straight forward.
  > NOTE: logic will be modified in Phase 2, to adapt to the WAL Raft log.


## TableDB Raft

### Interface
The client interface keeps unchanged, we provide a `script/TableDB/RaftSqliteStore.lua`, which is a `StorageProvider`. And we also changed the TableDatabase internal to adapt to the new Raft API,
RaftSqliteStore will send the Log Entry above to the raft cluster in each interface.

#### Config
To use TableDB Raft, u need to add a `tabledb.config.xml` file in the Tabledb `rootFolder`.
```xml
<tabledb>
	<providers>
		<provider type="TableDB.RaftSqliteStore" name="raft" file="(g1)npl_mod/TableDB/RaftSqliteStore.lua">./,localhost,9004,4
		</provider>
	</providers>
	<tables>
		<table provider="raft" name="User1"/>
		<table provider="raft" name="User2"/>
		<table provider="raft" name="default" />
	</tables>
</tabledb>
```
Above is a example of the `Raft` provider config.

* `provider` have 4 config,
  * `type` is used in `commonlib.gettable(provider.attr.type)`. 
  * `file` is used in `NPL.load(provider.attr.file)`. 
  * `name` is the name of the provider, which is used in the `table` config.
  * `values` is the init args used in provider `type`'s init method. 
* `table` have 2 config, 
  * `provider` is the name of the provider, correspond to the `name` in provider,
  * `name` is the table name. If the name is `default`, the corresponding provider will be set to be default provider of the database. If `default` is not set, default provider will be `sqlite`.


#### Callbacks
Like the original interfaces, callbacks is also implemented in a async way. But we make the connect(which is called in `RaftSqliteStore:init`) to be sync to get the Cluster's leader, this will boost the performance and alleviate the retry in subsequent request.

### Snapshot
We use sqlite's [Online Backup API](https://www.sqlite.org/backup.html) to make snapshot.


## Test TableDB/NPL Raft

### Start a 3 Raft Nodes cluster
 `cd setup && setup.bat` will start 3 Raft nodes. The 3 nodes will automatically elect a leader. we can stop one node(whether it is leader or not) and the cluster can still function correctly. Raft can tolerate `N/2 + 1` nodes failue, where N is the total nodes in cluster.

### Send Commands to the Cluster
Create `setup/client/temp/test_raft_database` directory, place the `tabledb.config.xml` above to the directory, and run `setup.bat client appendEntries`. This will start 1 client node whose tabledb `rootFolder` is `temp/test_raft_database` and default provider is `raft`.The client node will send commands to the cluster and automaticlly retry in a backoff way if the command not succeed. All 3 Raft nodes will recv the same commands. Whether succeed or not the client's callback will be called. 

### Add a server to the Cluster
`addsrv 5` will start a node whose id is 5.(Note: you need to delete the `server5` folder). To add the node to the cluster, execute `setup.bat client addServer 5`. The cluster will automatically sync the logs previously commited to this new added server. Now if you start a client to send commands to the cluster, the new server will also recv these commands.

> Note: every time you start a new client(whether Send Commands, Add server or Remove server), you should stop the previous client.

### Remove a server in the Cluster
`setup.bat client removeServer 5` will remove the node 5 in the cluster. But you can not request to remove the leader.


The Raft cluster is *fault tolerate* and *highly available*. You can stop the cluster and client with `stopNPL.bat`.

### Unit Test

```lua
NPL.load("(gl)script/ide/UnitTest/luaunit.lua");
NPL.load("(gl)npl_mod/Raft/test/TestClusterConfiguration.lua");
NPL.load("(gl)npl_mod/Raft/test/TestSnapshotSyncRequest.lua");
NPL.load("(gl)npl_mod/Raft/test/TestFileBasedSequentialLogStore.lua");
NPL.load("(gl)npl_mod/Raft/test/TestServerStateManager.lua");
NPL.load("(gl)npl_mod/TableDB/test/TestRaftLogEntryValue.lua");
LuaUnit:run('TestRaftLogEntryValue');
LuaUnit:run('TestFileBasedSequentialLogStore');
LuaUnit:run('TestClusterConfiguration');
LuaUnit:run('TestSnapshotSyncRequest');
LuaUnit:run('TestServerStateManager');
ParaGlobal.Exit(0)
```

Welcome for more feedbacks.:)
