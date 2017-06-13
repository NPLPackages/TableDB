--[[
Title: 
Author: liuluheng
Date: 2017.04.02
Desc: 
TEST
Cluster server configuration 
a class to hold the configuration information for a server in a cluster
------------------------------------------------------------
NPL.load("(gl)script/ide/UnitTest/luaunit.lua");
NPL.load("(gl)script/Raft/test/TestClusterConfiguration.lua");
LuaUnit:run('TestClusterConfiguration') 
------------------------------------------------------------
]]--

NPL.load("(gl)script/Raft/ClusterConfiguration.lua");
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration");
NPL.load("(gl)script/Raft/ClusterServer.lua");
local ClusterServer = commonlib.gettable("Raft.ClusterServer");

NPL.load("(gl)script/ide/UnitTest/luaunit.lua");


local MAX_LONG = 2^63 - 1;
local MAX_INT = 2^31 - 1;

TestClusterConfiguration = {}

function TestClusterConfiguration:testSerialization()
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

    local data = config:toBytes();
    local config1 = ClusterConfiguration:fromBytes(data);
    assertEquals(config.lastLogIndexl, config1.lastLogIndexl);
    assertEquals(config.logIndex, config1.logIndex);
    assertEquals(#config.servers, #config1.servers);

    for i,server in ipairs(config.servers) do
        local s1 = server;
        local s2 = config1.servers[i]
        assertEquals(s1.id, s2.id);
        assertEquals(s1.endpoint, s2.endpoint);
    end
end