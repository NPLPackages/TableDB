--[[
Title: 
Author: 
Date: 
Desc: 

Cluster server configuration 
a class to hold the configuration information for a server in a cluster
------------------------------------------------------------
NPL.load("(gl)script/Raft/ClusterConfiguration.lua");
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration");
------------------------------------------------------------
]]--


local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration");

function ClusterConfiguration:new(config) 
    local o = {
        logIndex = config.logIndex or 0,
        lastLogIndex = config.lastLogIndex or 0,
        servers = config.servers,
    };
    setmetatable(o, self);
    return o;
end

function ClusterConfiguration:__index(name)
    return rawget(self, name) or ClusterConfiguration[name];
end

function ClusterConfiguration:__tostring()
    return util.table_print(self)
end


function ClusterConfiguration:getServer(id)
    for _,server in ipairs(self.servers) do
        if(server.id == id) then
            return server;
        end
    end
end

function ClusterConfiguration:toBytes()
    return ;
end
