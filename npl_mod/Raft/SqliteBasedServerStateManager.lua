--[[
Title:
Author: liuluheng
Date: 2017.03.25
Desc:
    FileBased is better
    FIXME: use sqlite instead of TableDB

------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/SqliteBasedServerStateManager.lua");
local SqliteBasedServerStateManager = commonlib.gettable("Raft.SqliteBasedServerStateManager");
------------------------------------------------------------
]]
--
NPL.load("(gl)script/ide/Files.lua");
NPL.load("(gl)script/ide/Json.lua");
NPL.load("(gl)npl_mod/Raft/ClusterConfiguration.lua");
NPL.load("(gl)npl_mod/Raft/ServerState.lua");
NPL.load("(gl)script/ide/System/Database/TableDatabase.lua");
local TableDatabase = commonlib.gettable("System.Database.TableDatabase");
-- NPL.load("(gl)npl_mod/Raft/FileBasedSequentialLogStore.lua");
-- local FileBasedSequentialLogStore = commonlib.gettable("Raft.FileBasedSequentialLogStore");
NPL.load("(gl)npl_mod/Raft/WALSequentialLogStore.lua");
local WALSequentialLogStore = commonlib.gettable("Raft.WALSequentialLogStore");
local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua");
local ServerState = commonlib.gettable("Raft.ServerState");
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration");

local SqliteBasedServerStateManager = commonlib.gettable("Raft.SqliteBasedServerStateManager");

local SequentialLogStore = WALSequentialLogStore
local STATE_FILE = "server.state";
local CONFIG_FILE = "config.properties";
local CLUSTER_CONFIG_FILE = "cluster.json";


function SqliteBasedServerStateManager:new(dataDirectory)
    local o = {
        container = dataDirectory,
        logStore = SequentialLogStore:new(dataDirectory),
        logger = LoggerFactory.getLogger("SqliteBasedServerStateManager"),
        -- this will start both db client and db server if not.
        db = TableDatabase:new():connect(dataDirectory, function() end);
    };
    setmetatable(o, self);
    
    local configFile = ParaIO.open(o.container .. CONFIG_FILE, "r");
    if configFile:IsValid() then
        local line = configFile:readline()
        local index = string.find(line, "=")
        o.serverId = tonumber(string.sub(line, index + 1))
    end
    
    o.db:EnableSyncMode(true);
    
    -- o.db.serverState:insertOne(nil, {serverId=o.serverId});
    return o;
end

function SqliteBasedServerStateManager:__index(name)
    return rawget(self, name) or SqliteBasedServerStateManager[name];
end

function SqliteBasedServerStateManager:__tostring()
    return util.table_tostring(self)
end


-- Load cluster configuration for this server
function SqliteBasedServerStateManager:loadClusterConfiguration()
    local filename = self.container .. CLUSTER_CONFIG_FILE
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
function SqliteBasedServerStateManager:saveClusterConfiguration(configuration)
    local config = commonlib.Json.Encode(configuration);
    local filename = self.container .. CLUSTER_CONFIG_FILE
    local configFile = ParaIO.open(filename, "w");
    if configFile:IsValid() then
        configFile:WriteString(config);
        configFile:close()
    else
        self.logger.error("%s path error", filename)
    end
end


function SqliteBasedServerStateManager:persistState(serverState)
    self.logger.trace("persistState>term:%f,commitIndex:%f,votedFor:%f",
        serverState.term, serverState.commitIndex, serverState.votedFor);
    -- self.db.serverState:deleteOne({serverId = self.serverId});
    self.db.serverState:insertOne({serverId = self.serverId}, {
        serverId = self.serverId,
        term = serverState.term,
        commitIndex = serverState.commitIndex,
        votedFor = serverState.votedFor,
    });
end

function SqliteBasedServerStateManager:readState()
    local serverState;
    local err, data = self.db.serverState:findOne({serverId = self.serverId});
    if not err and data then
        serverState = ServerState:new(data.term, data.commitIndex, data.votedFor);
    else
        self.logger.error("persistState first");
    end
    
    return serverState;
end

function SqliteBasedServerStateManager:close()
    self.logStore:close();
end
