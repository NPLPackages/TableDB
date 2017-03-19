--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/RaftContext.lua");
local RaftContext = commonlib.gettable("Raft.RaftContext");
------------------------------------------------------------
]]--


local RaftContext = commonlib.gettable("Raft.RaftContext");

function RaftContext:new(stateManager, stateMachine, raftParameters, rpcListener,loggerFactory) 
    local o = {
        serverStateManager = stateManager,
        stateMachine = stateMachine,
        raftParameters = raftParameters,
        -- logger factory
        rpcListener = rpcListener,
        loggerFactory = loggerFactory,
    };
    setmetatable(o, self);
    return o;
end

function RaftContext:__index(name)
    return rawget(self, name) or RaftContext[name];
end

function RaftContext:__tostring()
    return util.table_tostring(self)
end

