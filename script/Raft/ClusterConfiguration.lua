--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 

Cluster server configuration 
a class to hold the configuration information for a server in a cluster
------------------------------------------------------------
NPL.load("(gl)script/Raft/ClusterConfiguration.lua");
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration");
------------------------------------------------------------
]]--

NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)script/Raft/ClusterServer.lua");
local ClusterServer = commonlib.gettable("Raft.ClusterServer");
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration");

function ClusterConfiguration:new(config)
    local o = {
        logIndex = (config and config.logIndex) or 0,
        lastLogIndex = (config and config.lastLogIndex) or 0,
        servers = {},
    };

    if config then
        for _,server in ipairs(config.servers) do
            o.servers[#o.servers + 1] = ClusterServer:new(server)
        end
    end

    setmetatable(o, self);
    return o;
end

function ClusterConfiguration:__index(name)
    return rawget(self, name) or ClusterConfiguration[name];
end

function ClusterConfiguration:__tostring()
    return util.table_tostring(self)
end

function ClusterConfiguration:getServer(id)
    for _,server in ipairs(self.servers) do
        if(server.id == id) then
            return server;
        end
    end
end

--[[
  De-serialize the data stored in buffer to cluster configuration
  this is used for the peers to get the cluster configuration from log entry value
  @param buffer the binary data
  @return cluster configuration
]]--
function ClusterConfiguration:fromBytes(bytes)
    local o = {
        servers = {},
    }
    local file = ParaIO.open("<memory>", "w");
    if(file:IsValid()) then	
        -- can not use file:WriteString(bytes);, use WriteBytes
        file:WriteBytes(#bytes, {bytes:byte(1, -1)});
        file:seek(0)
        o.logIndex = file:ReadDouble()
        o.lastLogIndex = file:ReadDouble()

        while file:getpos() < file:GetFileSize() do
            local server = {}
            server.id = file:ReadInt()
            local endpointLength = file:ReadInt()
            server.endpoint = file:ReadString(endpointLength)
            o.servers[#o.servers + 1] = ClusterServer:new(server)
        end
        file:close();
    end

    setmetatable(o, self);
    return o;
end


--[[
 Serialize the cluster configuration into a buffer
 this is used for the leader to serialize a new cluster configuration and replicate to peers
 @return binary data that represents the cluster configuration
]]--
function ClusterConfiguration:toBytes()
    -- "<memory>" is a special name for memory file, both read/write is possible. 
	local file = ParaIO.open("<memory>", "w");
    local bytes;
	if(file:IsValid()) then
        file:WriteDouble(self.logIndex)
        file:WriteDouble(self.lastLogIndex)

        for _,server in ipairs(self.servers) do
            local b = server:toBytes()
            file:WriteBytes(#b, {b:byte(1, -1)})
        end

        bytes = file:GetText(0, -1)

        file:close()
    end
    return bytes;
end
