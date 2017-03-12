--[[
Title: 
Author: 
Date: 
Desc: 

Cluster server configuration 
a class to hold the configuration information for a server in a cluster
------------------------------------------------------------
NPL.load("(gl)script/Raft.ClusterServer.lua");
local ClusterServer = commonlib.gettable("Raft.ClusterServer");
------------------------------------------------------------
]]--


local ClusterServer = commonlib.gettable("Raft.ClusterServer");

function ClusterServer:new() 
    local o = {
        id = 0,
        endpoint = "",
    };
    setmetatable(o, self);
    return o;
end

function ClusterServer:__index(name)
    return rawget(self, name) or ClusterServer[name];
end

function ClusterServer:__tostring()
    return format("ClusterServer(term:%d,commitIndex:%d,votedFor:%d)", self.term, self.commitIndex, self.votedFor);
end


function ClusterServer:toBytes()
    return ;
end
