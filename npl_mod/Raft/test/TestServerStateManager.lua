--[[
Title: 
Author: liuluheng
Date: 2017.04.05
Desc: 
TEST ServerStateManager
------------------------------------------------------------
NPL.load("(gl)script/ide/UnitTest/luaunit.lua");
NPL.load("(gl)npl_mod/Raft/test/TestServerStateManager.lua");
LuaUnit:run('TestServerStateManager') 
------------------------------------------------------------
]]--

NPL.load("(gl)npl_mod/Raft/ClusterConfiguration.lua");
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration");
NPL.load("(gl)npl_mod/Raft/ClusterServer.lua");
local ClusterServer = commonlib.gettable("Raft.ClusterServer");
NPL.load("(gl)npl_mod/Raft/ServerState.lua");
local ServerState = commonlib.gettable("Raft.ServerState");
NPL.load("(gl)npl_mod/Raft/ServerStateManager.lua");
local ServerStateManager = commonlib.gettable("Raft.ServerStateManager");


local MAX_LONG = 2^63 - 1;
local MAX_INT = 2^31 - 1;

local removeTestFiles, randomConfiguration, assertConfigEquals;

local assertTrue = assert


TestServerStateManager = {}

function TestServerStateManager:testStateManager()
    local container = "temp/logstore/";
    -- commonlib.Files.TouchFolder(container); -- this not works
    removeTestFiles(container);
    ParaIO.CreateDirectory(container);

    local serverId = math.random(MAX_INT);
    local config = randomConfiguration();
    
    local data = commonlib.Json.Encode(config);
    local filename = container.."cluster.json"
    local configFile = ParaIO.open(filename, "w");
    if configFile:IsValid() then
        configFile:WriteString(data);
        configFile:close();
    else
        self.logger.error("%s path error", filename)
    end

    local manager = ServerStateManager:new(container);
    assertTrue(manager.logStore ~= nil);
    assertTrue(manager:readState() == nil);
    local rounds = 50 + math.random(100);
    while(rounds > 0) do
        local state = ServerState:new(math.random(MAX_LONG), math.random(MAX_LONG), math.random(MAX_INT));
        manager:persistState(state);
        local state1 = manager:readState();
        assertTrue(state1 ~= nil);
        assertEquals(state.term, state1.term);
        assertEquals(state.commitIndex, state1.commitIndex);
        assertEquals(state.votedFor, state1.votedFor);
        rounds = rounds - 1;
    end

    local config1 = manager:loadClusterConfiguration();
    assertConfigEquals(config, config1);
    config = randomConfiguration();
    manager:saveClusterConfiguration(config);
    config1 = manager:loadClusterConfiguration();
    assertConfigEquals(config, config1);

    -- clean up
    manager:close();
    removeTestFiles(container);
end


function randomConfiguration()
    local config = ClusterConfiguration:new();
    config.lastLogIndex = math.random(MAX_LONG);
    config.logIndex = math.random(MAX_LONG);
    local servers = math.random(10) + 1;
    for i = 1, servers do
        local server = ClusterServer:new();
        server.id = math.random(MAX_INT);
        server.endpoint = string.format("Server %d", (i + 1));
        config.servers[#config.servers + 1] = server;
    end

    return config;  
end


function assertConfigEquals(config, config1)
    assertEquals(config.lastLogIndex, config1.lastLogIndex);
    assertEquals(config.logIndex, config1.logIndex);
    assertEquals(#config.servers, #config1.servers);
    for i = 1, #config.servers do
        local s1 = config.servers[i];
        local s2 = config.servers[i];
        assertEquals(s1.id, s2.id);
        assertEquals(s1.endpoint, s2.endpoint);
    end
end


function removeTestFiles(container)
    commonlib.Files.DeleteFolder(container);
end