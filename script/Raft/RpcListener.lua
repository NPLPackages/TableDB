--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft.RpcListener.lua");
local RpcListener = commonlib.gettable("Raft.RpcListener");
------------------------------------------------------------
]]--

NPL.load("(gl)script/ide/System/Concurrent/rpc.lua");
local rpc = commonlib.gettable("System.Concurrent.Async.rpc");



local RpcListener = commonlib.gettable("Raft.RpcListener");

function RpcListener:new() 
    local o = {
    };
    setmetatable(o, self);
    return o;
end

function RpcListener:__index(name)
    return rawget(self, name) or RpcListener[name];
end

function RpcListener:__tostring()
    return format("RpcListener(term:%d,commitIndex:%d,votedFor:%d)", self.term, self.commitIndex, self.votedFor);
end


--Starts listening and handle all incoming messages with messageHandler
function RpcListener:startListening(messageHandler)
    -- use rpc for incoming Request message
    rpc:new():init("RaftRequestRPC", function(self, msg) 
        LOG.std(nil, "info", "RaftRequestRPC", msg);
        msg = messageHandler.processRequest(msg)
        return msg; 
    end)

    -- use rpc for incoming Response message
    rpc:new():init("RaftResponseRPC", function(self, msg) 
        LOG.std(nil, "info", "RaftResponseRPC", msg);
        msg = messageHandler.processResponse(msg)
        return msg; 
    end)


end