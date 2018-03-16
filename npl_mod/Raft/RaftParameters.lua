--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/RaftParameters.lua");
local RaftParameters = commonlib.gettable("Raft.RaftParameters");
------------------------------------------------------------
]] --

local util = commonlib.gettable("System.Compiler.lib.util")
local RaftParameters = commonlib.gettable("Raft.RaftParameters")

function RaftParameters:new()
  local o = {
    -- Election timeout upper bound in milliseconds
    electionTimeoutUpperBound = 0,
    -- Election timeout lower bound in milliseconds
    electionTimeoutLowerBound = 0,
    -- heartbeat interval in milliseconds
    heartbeatInterval = 0,
    -- Rpc failure backoff in milliseconds
    rpcFailureBackoff = 0,
    -- For new member that just joined the cluster, we will use log sync to ask it to catch up,
    -- and this parameter is to specify how many log entries to pack for each sync request
    logSyncBatchSize = 0,
    -- For new member that just joined the cluster, we will use log sync to ask it to catch up,
    -- and this parameter is to tell when to stop using log sync but appendEntries for the new server
    -- when leaderCommitIndex - indexCaughtUp < logSyncStopGap, then appendEntries will be used
    logSyncStopGap = 0,
    -- log distance to compact between two snapshots
    snapshotDistance = 0,
    -- The tcp block size for syncing the snapshots
    snapshotBlockSize = 0,
    -- The maximum log entries could be attached to an appendEntries call
    maxAppendingSize = 0
  }
  setmetatable(o, self)
  return o
end

function RaftParameters:__index(name)
  return rawget(self, name) or RaftParameters[name]
end

function RaftParameters:__tostring()
  return util.table_tostring(self)
end

-- The maximum heartbeat interval, any value beyond this may lead to election timeout
-- for a peer before receiving a heartbeat
function RaftParameters:getMaxHeartbeatInterval()
  return math.max(self.heartbeatInterval, self.electionTimeoutLowerBound - self.heartbeatInterval / 2)
end
