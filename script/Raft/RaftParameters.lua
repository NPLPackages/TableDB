--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft.RaftParameters.lua");
local RaftParameters = commonlib.gettable("Raft.RaftParameters");
------------------------------------------------------------
]]--


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
    -- return format("RaftParameters(term:%d,commitIndex:%d,votedFor:%d)", self.term, self.commitIndex, self.votedFor);
    return util.table_print(self)
end


function RaftParameters:getMaxHeartbeatInterval()
    return math.max(self.heartbeatInterval, self.electionTimeoutLowerBound - self.heartbeatInterval / 2);
end