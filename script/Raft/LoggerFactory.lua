--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
local LoggerFactory = NPL.load("(gl)script/Raft/LoggerFactory.lua");
------------------------------------------------------------
]]--

NPL.load("(gl)script/Raft/Logger.lua");
local Logger = commonlib.gettable("Raft.Logger");

local LoggerFactory = NPL.export()

function LoggerFactory.getLogger(modname) 
    return Logger:new(modname);
end