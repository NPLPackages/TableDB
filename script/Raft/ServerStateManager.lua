--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft.ServerStateManager.lua");
local ServerStateManager = commonlib.gettable("Raft.ServerStateManager");
------------------------------------------------------------
]]--

NPL.load("(gl)script/ide/Files.lua");
local ServerStateManager = commonlib.gettable("Raft.ServerStateManager");


local STATE_FILE = "server.state";
local CONFIG_FILE = "config.properties";
local CLUSTER_CONFIG_FILE = "cluster.json";


function ServerStateManager:new(dataDirectory)

    local o = {
        serverStateFile = ParaIO.open(dataDirectory..STATE_FILE, "rw");
    };
    setmetatable(o, self);
    return o;
end

function ServerStateManager:__index(name)
    return rawget(self, name) or ServerStateManager[name];
end

function ServerStateManager:__tostring()
    -- return format("ServerStateManager(term:%d,commitIndex:%d,votedFor:%d)", self.term, self.commitIndex, self.votedFor);
    return util.table_print(self)
end


-- Load cluster configuration for this server
function ServerStateManager:loadClusterConfiguration()
    return ;
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