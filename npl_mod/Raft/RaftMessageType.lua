--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
local RaftMessageType = NPL.load("(gl)npl_mod/Raft/RaftMessageType.lua");
------------------------------------------------------------
]] --

local RaftMessageType = NPL.export()
--

-- mt = {}

--[[
  The equality comparison has some restrictions. If two objects have different
  basic types or different metamethods, the equality operation results in false,
  without even calling any metamethod.
]]
-- mt.__eq = function (a, b)
--   return a.int == b.int
-- end

local function rep(int, string)
  local o = {
    int = int,
    string = string
  }
  -- setmetatable(o, mt);
  return o
end

RaftMessageType.RequestVoteRequest = rep(0, "RequestVoteRequest")
RaftMessageType.RequestVoteResponse = rep(1, "RequestVoteResponse")
RaftMessageType.AppendEntriesRequest = rep(2, "AppendEntriesRequest")
RaftMessageType.AppendEntriesResponse = rep(3, "AppendEntriesResponse")
RaftMessageType.ClientRequest = rep(4, "ClientRequest")
RaftMessageType.AddServerRequest = rep(5, "AddServerRequest")
RaftMessageType.AddServerResponse = rep(6, "AddServerResponse")
RaftMessageType.RemoveServerRequest = rep(7, "RemoveServerRequest")
RaftMessageType.RemoveServerResponse = rep(8, "RemoveServerResponse")
RaftMessageType.SyncLogRequest = rep(9, "SyncLogRequest")
RaftMessageType.SyncLogResponse = rep(10, "SyncLogResponse")
RaftMessageType.JoinClusterRequest = rep(11, "JoinClusterRequest")
RaftMessageType.JoinClusterResponse = rep(12, "JoinClusterResponse")
RaftMessageType.LeaveClusterRequest = rep(13, "LeaveClusterRequest")
RaftMessageType.LeaveClusterResponse = rep(14, "LeaveClusterResponse")
RaftMessageType.InstallSnapshotRequest = rep(15, "InstallSnapshotRequest")
RaftMessageType.InstallSnapshotResponse = rep(16, "InstallSnapshotResponse")
