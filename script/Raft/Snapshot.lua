--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/Snapshot.lua");
local Snapshot = commonlib.gettable("Raft.Snapshot");
------------------------------------------------------------
]]--


local Snapshot = commonlib.gettable("Raft.Snapshot");

function Snapshot:new(lastLogIndex, lastLogTerm, lastConfig, size) 
    local o = {
        lastLogIndex = lastLogIndex,
        lastLogTerm = lastLogTerm,
        lastConfig = lastConfig,
        size = size or 0,
    };
    setmetatable(o, self);
    return o;
end

function Snapshot:__index(name)
    return rawget(self, name) or Snapshot[name];
end

function Snapshot:__tostring()
    return util.table_tostring(self)
end