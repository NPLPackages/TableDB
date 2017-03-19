--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/RpcClient.lua");
local RpcClient = commonlib.gettable("Raft.RpcClient");
------------------------------------------------------------
]]--


local RpcClient = commonlib.gettable("Raft.RpcClient");

function RpcClient:new() 
    local o = {
    };
    setmetatable(o, self);
    return o;
end

function RpcClient:__index(name)
    return rawget(self, name) or RpcClient[name];
end

function RpcClient:__tostring()
    -- return format("RpcClient(term:%d,commitIndex:%d,votedFor:%d)", self.term, self.commitIndex, self.votedFor);
    return util.table_tostring(self)
end



function RpcClient:send(request)

end