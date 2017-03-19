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

NPL.load("(gl)script/Raft/rpc.lua");
local rpc = commonlib.gettable("System.Concurrent.Async.rpc");
NPL.load("(gl)script/ide/socket/url.lua");
local url = commonlib.gettable("commonlib.socket.url")
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")

local RpcListener = commonlib.gettable("Raft.RpcListener");

function RpcListener:new(ip, port, servers) 
    local o = {
        ip = ip,
        port = port,
        servers = servers,
    };
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
    -- use rpc for incoming Request message
    rpc:new():init("RaftRequestRPC", function(self, msg) 
        LOG.std(nil, "info", "RaftRequestRPC", msg);
        msg = messageHandler.processRequest(msg)
        return msg; 
    end)

    -- port is need to be string here??
    NPL.StartNetServer(self.ip, tostring(self.port));

    for _, server in ipairs(self.servers) do
        local parsed_url = url.parse(server.endpoint)
        NPL.AddNPLRuntimeAddress({host = parsed_url.host, port = tostring(parsed_url.port), nid = server.endpoint})
    end
    RaftRequestRPC:MakePublic();




    -- XXX: need handle response here??
    -- use rpc for incoming Response message
    -- rpc:new():init("RaftResponseRPC", function(self, msg) 
    --     LOG.std(nil, "info", "RaftResponseRPC", msg);
    --     msg = messageHandler.processResponse(msg)
    --     return msg; 
    -- end)

    -- RaftResponseRPC:MakePublic();


end