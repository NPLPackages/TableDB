--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
local ServerRole = NPL.load("(gl)script/Raft/ServerRole.lua");
------------------------------------------------------------
]]--


local ServerRole = NPL.export();

ServerRole.Follower  = 0
ServerRole.Candidate = 1
ServerRole.Leader	   = 2