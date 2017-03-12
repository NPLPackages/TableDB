--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft.RaftContext.lua");
local RaftContext = commonlib.gettable("Raft.RaftContext");
------------------------------------------------------------
]]--


local RaftContext = commonlib.gettable("Raft.RaftContext");

function RaftContext:new(ctx) 
    local o = {
        serverStateManager = ctx.serverStateManager,
        stateMachine = ctx.stateMachine,
        raftParameters = ctx.stateMachine,
        -- logger factory
    };
    setmetatable(o, self);
    return o;
end

function RaftContext:__index(name)
    return rawget(self, name) or RaftContext[name];
end

function RaftContext:__tostring()
    -- return format("RaftContext(term:%d,commitIndex:%d,votedFor:%d)", self.term, self.commitIndex, self.votedFor);
end

