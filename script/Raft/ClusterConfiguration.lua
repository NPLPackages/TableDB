--[[
Title: 
Author: 
Date: 
Desc: 

Cluster server configuration 
a class to hold the configuration information for a server in a cluster
------------------------------------------------------------
NPL.load("(gl)script/Raft.ClusterConfiguration.lua");
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration");
------------------------------------------------------------
]]--


local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration");

function ClusterConfiguration:new() 
    local o = {
        logIndex = 0,
        lastLogIndex = 0,
        servers = {
            
        }
    };
    setmetatable(o, self);
    return o;
end

function ClusterConfiguration:__index(name)
    return rawget(self, name) or ClusterConfiguration[name];
end

function ClusterConfiguration:__tostring()
    -- return format("ClusterConfiguration(term:%d,commitIndex:%d,votedFor:%d)", self.term, self.commitIndex, self.votedFor);
    return util.table_print(self)
end


function ClusterConfiguration:toBytes()
    return ;
end
