--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/ServerStateManager.lua");
local ServerStateManager = commonlib.gettable("Raft.ServerStateManager");
------------------------------------------------------------
]]--

NPL.load("(gl)script/ide/Files.lua");
NPL.load("(gl)script/ide/Json.lua");
NPL.load("(gl)script/Raft/ClusterConfiguration.lua");
NPL.load("(gl)script/Raft/ServerState.lua");
NPL.load("(gl)script/Raft/SequentialLogStore.lua");
local LoggerFactory = NPL.load("(gl)script/Raft/LoggerFactory.lua");
local SequentialLogStore = commonlib.gettable("Raft.SequentialLogStore");
local ServerState = commonlib.gettable("Raft.ServerState");
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration");
local ServerStateManager = commonlib.gettable("Raft.ServerStateManager");

local STATE_FILE = "server.state";
local CONFIG_FILE = "config.properties";
local CLUSTER_CONFIG_FILE = "cluster.json";


function ServerStateManager:new(dataDirectory)
    local o = {
        container = dataDirectory,
        logStore = SequentialLogStore:new(dataDirectory),
        logger = LoggerFactory.getLogger("ServerStateManager")
    };
    setmetatable(o, self);

    local configFile = ParaIO.open(o.container..CONFIG_FILE, "r");
    if configFile:IsValid() then
        local line = configFile:readline()
        local index = string.find(line, "=")
        o.serverId = tonumber(string.sub(line, index+1))
    end

    -- stateFile 
    if not ParaIO.DoesFileExist(o.container..STATE_FILE) then
        local result = ParaIO.CreateNewFile(o.container..STATE_FILE)
        assert(result, "create serverStateFile failed")
    end

    o.serverStateFile = ParaIO.open(dataDirectory..STATE_FILE, "rw");

    assert(o.serverStateFile:IsValid(), "serverStateFile not Valid")
    o.serverStateFile:seek(0)

    return o;
end

function ServerStateManager:__index(name)
    return rawget(self, name) or ServerStateManager[name];
end

function ServerStateManager:__tostring()
    return util.table_tostring(self)
end


-- Load cluster configuration for this server
function ServerStateManager:loadClusterConfiguration()
    local filename = self.container..CLUSTER_CONFIG_FILE
    local configFile = ParaIO.open(filename, "r");
    if configFile:IsValid() then
        local text = configFile:GetText();
        local config = commonlib.Json.Decode(text);
        return ClusterConfiguration:new(config);
    else
        self.logger.error("%s path error", filename)
    end
end

-- Save cluster configuration
function ServerStateManager:saveClusterConfiguration(configuration)
    local config = commonlib.Json.Encode(configuration);
    local filename = self.container..CLUSTER_CONFIG_FILE
    local configFile = ParaIO.open(filename, "w");
    if configFile:IsValid() then
        configFile:WriteString(config);
    else
        self.logger.error("%s path error", filename)
    end
end


function ServerStateManager:persistState(serverState)
    -- self.logger.info("persistState:".. serverState.term .. serverState.commitIndex .. serverState.votedFor)
    self.serverStateFile:WriteUInt(serverState.term)
    self.serverStateFile:WriteUInt(serverState.commitIndex)
    self.serverStateFile:WriteInt(serverState.votedFor)
    self.serverStateFile:seek(0)
end

function ServerStateManager:readState()
    if(self.serverStateFile:GetFileSize() == 0) then
        return nil
    end

    local term = self.serverStateFile:ReadUint()
    local commitIndex = self.serverStateFile:ReadUint()
    local votedFor = self.serverStateFile:Readint()

    return ServerState:new(term, commitIndex, votedFor);
end
