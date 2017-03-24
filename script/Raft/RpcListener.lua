--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/RpcListener.lua");
local RpcListener = commonlib.gettable("Raft.RpcListener");
------------------------------------------------------------
]]--

NPL.load("(gl)script/Raft/Rpc.lua");
local Rpc = commonlib.gettable("Raft.Rpc");
NPL.load("(gl)script/ide/socket/url.lua");
local url = commonlib.gettable("commonlib.socket.url")
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
local LoggerFactory = NPL.load("(gl)script/Raft/LoggerFactory.lua");

local RpcListener = commonlib.gettable("Raft.RpcListener");

function RpcListener:new(ip, port, servers) 
    local o = {
        ip = ip,
        port = port,
        servers = servers,
        logger = LoggerFactory.getLogger("RpcListener"),
    };
    
    for _, server in ipairs(o.servers) do
        local parsed_url = url.parse(server.endpoint)
        NPL.AddNPLRuntimeAddress({host = parsed_url.host, port = tostring(parsed_url.port), nid = "server"..server.id})
    end
    NPL.AddNPLRuntimeAddress({host = "localhost", port = "9004", nid = "server4"})

    setmetatable(o, self);
    return o;
end

function RpcListener:__index(name)
    return rawget(self, name) or RpcListener[name];
end

function RpcListener:__tostring()
    return util.table_tostring(self)
end


--Starts listening and handle all incoming messages with messageHandler
function RpcListener:startListening(messageHandler)
    self.logger.info("startListening")
    -- use Rpc for incoming Request message
    Rpc:new():init("RaftRequestRPC", function(self, msg) 
        -- LOG.std(nil, "debug", "RaftRequestRPC", msg);
        -- self.logger.info("RaftRequestRPC:%s",util.table_tostring(msg));
        msg = messageHandler:processRequest(msg)
        return msg; 
    end)

    -- port is need to be string here??
    NPL.StartNetServer(self.ip, tostring(self.port));

    -- for _, server in ipairs(self.servers) do
    --     local parsed_url = url.parse(server.endpoint)
    --     NPL.AddNPLRuntimeAddress({host = parsed_url.host, port = tostring(parsed_url.port), nid = "server"..server.id})
    -- end
    RaftRequestRPC:MakePublic();

end