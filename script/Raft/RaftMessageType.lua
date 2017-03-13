--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
local RaftMessageType = NPL.load("(gl)script/Raft.RaftMessageType.lua");
------------------------------------------------------------
]]--


local RaftMessageType = NPL.export();

RaftMessageType.RequestVoteRequest  = 0
RaftMessageType.RequestVoteResponse = 1
RaftMessageType.AppendEntriesRequest       = 2
RaftMessageType.AppendEntriesResponse       = 3
RaftMessageType.ClientRequest       = 4
RaftMessageType.AddServerRequest       = 5
RaftMessageType.AddServerResponse       = 6
RaftMessageType.RemoveServerRequest       = 7
RaftMessageType.RemoveServerResponse       = 8
RaftMessageType.SyncLogRequest       = 9
RaftMessageType.SyncLogResponse       = 10
RaftMessageType.JoinClusterRequest       = 11
RaftMessageType.JoinClusterResponse       = 12
RaftMessageType.LeaveClusterRequest       = 13
RaftMessageType.LeaveClusterResponse       = 14
RaftMessageType.InstallSnapshotRequest       = 15
RaftMessageType.InstallSnapshotResponse       = 16