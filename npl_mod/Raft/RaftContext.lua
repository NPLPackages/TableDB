--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/RaftContext.lua");
local RaftContext = commonlib.gettable("Raft.RaftContext");
------------------------------------------------------------
]]--


local RaftContext = commonlib.gettable("Raft.RaftContext");

function RaftContext:new(stateManager, stateMachine, raftParameters, rpcListener,loggerFactory) 
    local o = {
        serverStateManager = stateManager,
        stateMachine = stateMachine,
        raftParameters = raftParameters,
        rpcListener = rpcListener,
        -- logger factory
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

