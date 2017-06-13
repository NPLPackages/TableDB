--[[
Title: 
Author: liuluheng
Date: 2017.04.02
Desc: 
TEST SnapshotSyncRequest
------------------------------------------------------------
NPL.load("(gl)script/ide/UnitTest/luaunit.lua");
NPL.load("(gl)npl_mod/Raft/test/TestSnapshotSyncRequest.lua");
LuaUnit:run('TestSnapshotSyncRequest') 
------------------------------------------------------------
]]--

NPL.load("(gl)npl_mod/Raft/ClusterConfiguration.lua");
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration");
NPL.load("(gl)npl_mod/Raft/Snapshot.lua");
local Snapshot = commonlib.gettable("Raft.Snapshot");
NPL.load("(gl)npl_mod/Raft/SnapshotSyncRequest.lua");
local SnapshotSyncRequest = commonlib.gettable("Raft.SnapshotSyncRequest");
NPL.load("(gl)npl_mod/Raft/ClusterServer.lua");
local ClusterServer = commonlib.gettable("Raft.ClusterServer");

NPL.load("(gl)npl_mod/Raft/test/TestUtil.lua");
NPL.load("(gl)script/ide/UnitTest/luaunit.lua");


local MAX_LONG = 2^63 - 1;
local MAX_INT = 2^31 - 1;

TestSnapshotSyncRequest = {}

function TestSnapshotSyncRequest:testSerialization()
    local config = ClusterConfiguration:new();
    math.randomseed(os.time())
    config.lastLogIndex = math.random(MAX_LONG);
    config.logIndex = math.random(MAX_LONG);
    local servers = math.random(10);
    for i=1,servers do
      local server = ClusterServer:new()
      server.id = math.random(MAX_INT)
      server.endpoint = string.format( "Server %d", (i + 1))
      config.servers[#config.servers + 1] = server
    end

    local snapshot = Snapshot:new(math.random(MAX_LONG), math.random(MAX_LONG), config);
    local snapshotData = string.random(math.random(200), "%l%d");
    local randomBoolean = (math.random(2) % 2 == 0 and true) or false;

    local request = SnapshotSyncRequest:new(snapshot, math.random(MAX_LONG), snapshotData, randomBoolean);
    local data = request:toBytes();
    local request1 = SnapshotSyncRequest:fromBytes(data);
    assertEquals(request.offset, request1.offset);
    assertEquals(request.done, request1.done);
    local snapshot1 = request1.snapshot;
    assertEquals(snapshot.lastLogIndex, snapshot1.lastLogIndex);
    assertEquals(snapshot.lastLogTerm, snapshot1.lastLogTerm);
    local config1 = snapshot1.lastConfig;
    assertEquals(config.lastLogIndex, config1.lastLogIndex);
    assertEquals(config.logIndex, config1.logIndex);
    assertEquals(#config.servers, #config1.servers);

    for i,server in ipairs(config.servers) do
        local s1 = server;
        local s2 = config1.servers[i];
        assertEquals(s1.id, s2.id);
        assertEquals(s1.endpoint, s2.endpoint);
    end

    local snapshotData1 = request1.data;
    assertEquals(#snapshotData, #snapshotData1);

    for i = 1, #snapshotData do
        assertEquals(snapshotData[i], snapshotData1[i]);
    end
end

function TestSnapshotSyncRequest:testSerializationWithZeroData()
    local config = ClusterConfiguration:new();
    math.randomseed(os.time())
    config.lastLogIndex = math.random(MAX_LONG);
    config.logIndex = math.random(MAX_LONG);
    local servers = math.random(10);
    for i=1,servers do
      local server = ClusterServer:new()
      server.id = math.random(MAX_INT)
      server.endpoint = string.format( "Server %d", (i + 1))
      config.servers[#config.servers + 1] = server
    end

    local snapshot = Snapshot:new(math.random(MAX_LONG), math.random(MAX_LONG), config);
    local snapshotData = {}
    local randomBoolean = (math.random(2) % 2 == 0 and true) or false;

    local request = SnapshotSyncRequest:new(snapshot, math.random(MAX_LONG), snapshotData, randomBoolean);
    local data = request:toBytes();
    local request1 = SnapshotSyncRequest:fromBytes(data);
    assertEquals(request.offset, request1.offset);
    assertEquals(request.done, request1.done);
    local snapshot1 = request1.snapshot;
    assertEquals(snapshot.lastLogIndex, snapshot1.lastLogIndex);
    assertEquals(snapshot.lastLogTerm, snapshot1.lastLogTerm);
    local config1 = snapshot1.lastConfig;
    assertEquals(config.lastLogIndex, config1.lastLogIndex);
    assertEquals(config.logIndex, config1.logIndex);
    assertEquals(#config.servers, #config1.servers);

    for i,server in ipairs(config.servers) do
        local s1 = server;
        local s2 = config1.servers[i];
        assertEquals(s1.id, s2.id);
        assertEquals(s1.endpoint, s2.endpoint);
    end

    local snapshotData1 = request1.data;
    assertEquals(#snapshotData, #snapshotData1);
end