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
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration");
local ServerStateManager = commonlib.gettable("Raft.ServerStateManager");
local logger = commonlib.logging.GetLogger("")

local STATE_FILE = "server.state";
local CONFIG_FILE = "config.properties";
local CLUSTER_CONFIG_FILE = "cluster.json";


function ServerStateManager:new(dataDirectory)

    local o = {
        container = dataDirectory,
        -- serverStateFile = ParaIO.open(dataDirectory..STATE_FILE, "rw");
    };
    setmetatable(o, self);
    return o;
end

function ServerStateManager:__index(name)
    return rawget(self, name) or ServerStateManager[name];
end

function ServerStateManager:__tostring()
    return util.table_print(self)
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
        logger.error("%s path error", filename)
    end
end

-- Save cluster configuration
function ServerStateManager:saveClusterConfiguration(configuration)
    -- json config
    return ;
end


function ServerStateManager:persistState(serverState)
    -- json config
    return ;
end

function ServerStateManager:readState()
    -- json config
    return ;
end

function ServerStateManager:loadLogStore()
    -- json config
    return ;
end


function ServerStateManager:getServerId()
    -- json config
    return ;
end