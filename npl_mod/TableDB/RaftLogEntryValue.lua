--[[
Title:
Author: liuluheng
Date: 2017.04.12
Desc:


------------------------------------------------------------
NPL.load("(gl)npl_mod/TableDB/RaftLogEntryValue.lua");
local RaftLogEntryValue = commonlib.gettable("TableDB.RaftLogEntryValue");
------------------------------------------------------------
]]
--
NPL.load("(gl)script/ide/commonlib.lua")
local RaftLogEntryValue = commonlib.gettable("TableDB.RaftLogEntryValue")
NPL.load("(gl)npl_mod/TableDB/VectorPool.lua")
local VectorPool = commonlib.gettable("TableDB.VectorPool")

local memoryFile
-- set this to true, if one wants to use binary format (currently slower than text format)
local is_binary_format = false
if (is_binary_format) then
  memoryFile = ParaIO.open("<memory>", "w")
end

function RaftLogEntryValue:new(
  client_uid,
  query_type,
  db,
  collectionName,
  query,
  index,
  serverId,
  enableSyncMode,
  callbackThread)
  local o = {
    client_uid = client_uid,
    query_type = query_type,
    db = db,
    collectionName = collectionName,
    query = query,
    cb_index = index,
    serverId = serverId,
    enableSyncMode = enableSyncMode,
    callbackThread = callbackThread
  }
  setmetatable(o, self)
  return o
end

function RaftLogEntryValue:new_from_pool(
  client_uid,
  query_type,
  db,
  collectionName,
  query,
  index,
  serverId,
  enableSyncMode,
  callbackThread)
  return VectorPool.GetSingleton():GetVector(
    client_uid,
    query_type,
    db,
    collectionName,
    query,
    index,
    serverId,
    enableSyncMode,
    callbackThread
  )
end

function RaftLogEntryValue:set(
  client_uid,
  query_type,
  db,
  collectionName,
  query,
  index,
  serverId,
  enableSyncMode,
  callbackThread)
  self.client_uid = client_uid
  self.query_type = query_type
  self.db = db
  self.collectionName = collectionName
  self.query = query
  self.cb_index = index
  self.serverId = serverId
  self.enableSyncMode = enableSyncMode
  self.callbackThread = callbackThread
end

function RaftLogEntryValue:__index(name)
  return rawget(self, name) or RaftLogEntryValue[name]
end

function RaftLogEntryValue:__tostring()
  return util.table_tostring(self)
end

-- this is actual msg table we used for exchanging data, slightly more compact
local msg_ = {}
function RaftLogEntryValue:fromBytes(bytes)
  if (is_binary_format) then
    if (memoryFile:IsValid()) then
      memoryFile:seek(0)
      if type(bytes) == "string" then
        memoryFile:write(bytes, #bytes)
      elseif type(bytes) == "table" then
        memoryFile:WriteBytes(#bytes, bytes)
      end
      memoryFile:seek(0)

      local n = memoryFile:ReadInt()
      local str = memoryFile:ReadString(n)
      -- print(str)
      local o = commonlib.LoadTableFromString(str)
      if not o then
        str = string.gsub(str, "([%+%-][%a+%+%-]+)", '["%1"]')
        o = commonlib.LoadTableFromString(str)
      end
      setmetatable(o, self)
      return o
    end
  else
    local t = NPL.LoadTableFromString(bytes)
    if (t) then
      -- decode from msg_
      -- this is slow.. why?
      -- return RaftLogEntryValue:new_from_pool(t[1], t[2], t[3], t[4], t[5], t[6], t[7], t[8])
      local o = {
        client_uid = t[1],
        query_type = t[2],
        db = t[3],
        collectionName = t[4],
        query = t[5],
        cb_index = t[6],
        serverId = t[7],
        enableSyncMode = t[8],
        callbackThread = t[9]
      }
      setmetatable(o, self)
      return o
    else
      LOG.std(nil, "warn", "RaftLogEntryValue:fromBytes", "can not parse bytes")
    end
  end
end

function RaftLogEntryValue:toBytes()
  if (is_binary_format) then
    local str = commonlib.serialize_compact(self)

    local bytes
    if (memoryFile:IsValid()) then
      -- -- query_type
      -- memoryFile:WriteInt(#self.query_type)
      -- memoryFile:WriteString(self.query_type)
      -- -- collection
      -- memoryFile:WriteInt(#self.collection.name)
      -- memoryFile:WriteString(self.collection.name)
      -- memoryFile:WriteInt(#self.collection.db)
      -- memoryFile:WriteString(self.collection.db)
      -- query
      -- query is a dict or a nested dict
      memoryFile:seek(0)
      local nSize = #str
      memoryFile:WriteInt(nSize)
      memoryFile:write(str, nSize)
      bytes = memoryFile:GetText(0, memoryFile:getpos())
    end
    return bytes
  else
    -- encode into msg_
    msg_[1] = self.client_uid
    msg_[2] = self.query_type
    msg_[3] = self.db
    msg_[4] = self.collectionName
    msg_[5] = self.query
    msg_[6] = self.cb_index
    msg_[7] = self.serverId
    msg_[8] = self.enableSyncMode
    msg_[9] = self.callbackThread
    return commonlib.serialize_compact(msg_)
  end
end
