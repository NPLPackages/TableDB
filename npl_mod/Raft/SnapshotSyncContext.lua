--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/SnapshotSyncContext.lua");
local SnapshotSyncContext = commonlib.gettable("Raft.SnapshotSyncContext");
------------------------------------------------------------
]] --

local SnapshotSyncContext = commonlib.gettable("Raft.SnapshotSyncContext")

function SnapshotSyncContext:new(snapshot, currentCollectionIndex)
  local o = {
    snapshot = snapshot,
    offset = 0,
    -- extended for TableDB collections
    currentCollectionIndex = currentCollectionIndex or 1
  }
  setmetatable(o, self)
  return o
end

function SnapshotSyncContext:__index(name)
  return rawget(self, name) or SnapshotSyncContext[name]
end

function SnapshotSyncContext:__tostring()
  return util.table_tostring(self)
end
