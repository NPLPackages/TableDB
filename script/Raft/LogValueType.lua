--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
local LogValueType = NPL.load("(gl)script/Raft/LogValueType.lua");
------------------------------------------------------------
]]--


local LogValueType = NPL.export();

LogValueType.Application  = 0
LogValueType.Configuration = 1
LogValueType.ClusterServer       = 2
LogValueType.LogPack       = 3
LogValueType.SnapshotSyncRequest       = 4