--[[
Title:
Author: liuluheng
Date: 2017.04.12
Desc:


------------------------------------------------------------
NPL.load("(gl)npl_mod/TableDB/RaftWALLogEntryValue.lua");
local RaftWALLogEntryValue = commonlib.gettable("TableDB.RaftWALLogEntryValue");
------------------------------------------------------------
]]
--
NPL.load("(gl)script/ide/commonlib.lua")
local RaftWALLogEntryValue = commonlib.gettable("TableDB.RaftWALLogEntryValue")

local memoryFile = ParaIO.open("<memory>", "rw")

function RaftWALLogEntryValue:new(rootFolder, collectionName, page_data, pgno, nTruncate, isCommit)
  local o = {
    rootFolder = rootFolder,
    collectionName = collectionName,
    page_data = page_data,
    pgno = pgno,
    nTruncate = nTruncate,
    isCommit = isCommit
  }
  setmetatable(o, self)
  return o
end

function RaftWALLogEntryValue:__index(name)
  return rawget(self, name) or RaftWALLogEntryValue[name]
end

function RaftWALLogEntryValue:__tostring()
  return util.table_tostring(self)
end

function RaftWALLogEntryValue:fromBytes(bytes)
  local o = {}
  if (memoryFile:IsValid()) then
    memoryFile:seek(0)
    -- local bytes = {bytes:byte(1, -1)}
    -- write not worked! use error ? string:char maybe
    -- memoryFile:write(bytes, #bytes)
    -- memoryFile:WriteBytes(#bytes, bytes)

    if type(bytes) == "string" then
      -- file:WriteBytes(#bytes, {bytes:byte(1, -1)});
      memoryFile:write(bytes, #bytes)
    elseif type(bytes) == "table" then
      memoryFile:WriteBytes(#bytes, bytes)
    end

    memoryFile:seek(0)

    -- rootFolder
    local rootFolderLen = memoryFile:ReadInt()
    o.rootFolder = memoryFile:ReadString(rootFolderLen)
    -- collectionName
    local collectionNameLen = memoryFile:ReadInt()
    o.collectionName = memoryFile:ReadString(collectionNameLen)
    -- page_data
    local page_data_len = memoryFile:ReadInt()
    o.page_data = {}
    memoryFile:ReadBytes(page_data_len, o.page_data)
    o.page_data = string.char(unpack(o.page_data))
    -- pgno
    o.pgno = memoryFile:ReadInt()
    -- nTruncate
    o.nTruncate = memoryFile:ReadInt()
    -- isCommit
    o.isCommit = memoryFile:ReadInt()

    setmetatable(o, self)
    return o
  end
end

function RaftWALLogEntryValue:toBytes()
  local bytes
  if (memoryFile:IsValid()) then
    memoryFile:seek(0)

    -- rootFolder
    memoryFile:WriteInt(#self.rootFolder)
    memoryFile:WriteString(self.rootFolder)
    -- collectionName
    memoryFile:WriteInt(#self.collectionName)
    memoryFile:WriteString(self.collectionName)
    -- page_data
    -- local page_data_bytes = {self.page_data:byte(1, -1)}
    -- memoryFile:WriteInt(#page_data_bytes)
    -- memoryFile:WriteBytes(#page_data_bytes, page_data_bytes)
    memoryFile:WriteInt(#self.page_data)
    memoryFile:write(self.page_data, #self.page_data)

    -- pgno
    memoryFile:WriteInt(self.pgno)
    -- nTruncate
    memoryFile:WriteInt(self.nTruncate)
    -- isCommit
    memoryFile:WriteInt(self.isCommit)

    bytes = memoryFile:GetText(0, -1)
  end
  return bytes
end
