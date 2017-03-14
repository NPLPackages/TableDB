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



function RpcListener:startListening(messageHandler)
    -- rpc:new():init("Test.testRPC", function(self, msg) 
    --     LOG.std(nil, "info", "category", msg);
    --     msg.output=true; 
    --     ParaEngine.Sleep(1);
    --     return msg; 
    -- end)
end