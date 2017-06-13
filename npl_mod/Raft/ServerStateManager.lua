--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/ServerStateManager.lua");
local ServerStateManager = commonlib.gettable("Raft.ServerStateManager");
------------------------------------------------------------
]]--

NPL.load("(gl)script/ide/Files.lua");
NPL.load("(gl)script/ide/Json.lua");
NPL.load("(gl)npl_mod/Raft/ClusterConfiguration.lua");
NPL.load("(gl)npl_mod/Raft/ServerState.lua");
NPL.load("(gl)npl_mod/Raft/FileBasedSequentialLogStore.lua");
local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua");
local FileBasedSequentialLogStore = commonlib.gettable("Raft.FileBasedSequentialLogStore");
local ServerState = commonlib.gettable("Raft.ServerState");
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration");
local ServerStateManager = commonlib.gettable("Raft.ServerStateManager");

local STATE_FILE = "server.state";
local CONFIG_FILE = "config.properties";
local CLUSTER_CONFIG_FILE = "cluster.json";


function ServerStateManager:new(dataDirectory)
    local o = {
        container = dataDirectory,
        logStore = FileBasedSequentialLogStore:new(dataDirectory),
        logger = LoggerFactory.getLogger("ServerStateManager")
    };
    setmetatable(o, self);

    local configFile = ParaIO.open(o.container..CONFIG_FILE, "r");
    if configFile:IsValid() then
        local line = configFile:readline()
        local index = string.find(line, "=")
        o.serverId = tonumber(string.sub(line, index+1))
    end
    
    o.serverStateFileName = o.container..STATE_FILE;
    o.serverStateFile = ParaIO.open(o.serverStateFileName, "rw");
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
        configFile:close()
    else
        self.logger.error("%s path error", filename)
    end
end


function ServerStateManager:persistState(serverState)
    self.logger.trace("ServerStateManager:persistState>term:%f,commitIndex:%f,votedFor:%f", 
                        serverState.term, serverState.commitIndex, serverState.votedFor)
    self.serverStateFile:WriteDouble(serverState.term)
    self.serverStateFile:WriteDouble(serverState.commitIndex)
    self.serverStateFile:WriteInt(serverState.votedFor)
    self.serverStateFile:SetEndOfFile()
    self.serverStateFile:seek(0)
end

function ServerStateManager:readState()
    self.serverStateFile:close();

    local serverStateFile = ParaIO.open(self.serverStateFileName, "r");
    if(serverStateFile:GetFileSize() == 0) then
        self.serverStateFile = ParaIO.open(self.serverStateFileName, "rw");
        assert(self.serverStateFile:IsValid(), "serverStateFile not Valid")
        self.serverStateFile:seek(0)
        return;
    end

    local term = serverStateFile:ReadDouble()
    local commitIndex = serverStateFile:ReadDouble()
    local votedFor = serverStateFile:ReadInt()

    serverStateFile:close();
    
    self.serverStateFile = ParaIO.open(self.serverStateFileName, "rw");
    assert(self.serverStateFile:IsValid(), "serverStateFile not Valid")
    self.serverStateFile:seek(0)
    return ServerState:new(term, commitIndex, votedFor);
end

function ServerStateManager:close()
    self.serverStateFile:close();
    self.logStore:close();
end