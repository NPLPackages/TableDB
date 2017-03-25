--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/RaftParameters.lua");
local RaftParameters = commonlib.gettable("Raft.RaftParameters");
------------------------------------------------------------
]]--

local util = commonlib.gettable("System.Compiler.lib.util")
local RaftParameters = commonlib.gettable("Raft.RaftParameters");

function RaftParameters:new() 
    local o = {
        electionTimeoutUpperBound = 0;
        electionTimeoutLowerBound = 0;
        heartbeatInterval = 0;
        rpcFailureBackoff = 0;
        logSyncBatchSize = 0;
        logSyncStopGap = 0;
        snapshotDistance = 0;
        snapshotBlockSize = 0;
        maxAppendingSize = 0;
    };
    setmetatable(o, self);
    return o;
end

function RaftParameters:__index(name)
    return rawget(self, name) or RaftParameters[name];
end

function RaftParameters:__tostring()
    return util.table_tostring(self)
end


function RaftParameters:getMaxHeartbeatInterval()
    return math.max(self.heartbeatInterval, self.electionTimeoutLowerBound - self.heartbeatInterval / 2);
end