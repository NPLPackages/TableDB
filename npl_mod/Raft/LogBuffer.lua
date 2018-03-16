--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 
    the LogBuffer is startIndex based
------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/LogBuffer.lua");
local LogBuffer = commonlib.gettable("Raft.LogBuffer");
------------------------------------------------------------
]] --

local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua")
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua")
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)npl_mod/Raft/Rutils.lua")
local Rutils = commonlib.gettable("Raft.Rutils")
local LogBuffer = commonlib.gettable("Raft.LogBuffer")

function LogBuffer:new(startIndex, maxSize)
  local o = {
    startIndex = startIndex,
    entriesInBuffer = 0,
    maxSize = maxSize,
    logger = LoggerFactory.getLogger("LogBuffer"),
    buffer = {}
  }
  setmetatable(o, self)
  return o
end

function LogBuffer:__index(name)
  return rawget(self, name) or LogBuffer[name]
end

function LogBuffer:__tostring()
  return util.table_tostring(self)
end

function LogBuffer:lastIndex()
  return self.startIndex + self.entriesInBuffer - 1
end

function LogBuffer:firstIndex()
  return self.startIndex
end

function LogBuffer:lastEntry()
  -- if buffer size = 0, will return nil
  return self.buffer[self:lastIndex()]
end

function LogBuffer:entryAt(index)
  self.logger.trace(
    "entryAt>index:%d, self.startIndex:%d, self.buffer len:%d",
    index,
    self.startIndex,
    self:bufferSize()
  )
  return self.buffer[index]
end

-- [start, end), returns the startIndex
function LogBuffer:fill(start, endi, result)
  if endi < self.startIndex then
    return self.startIndex
  end

  if start < self.startIndex then
    start = self.startIndex
  end
  for i = start, endi - 1 do
    result[#result + 1] = self:entryAt(i)
  end

  self.logger.trace(
    "fill>start:%d, end:%d, result len:%d, self.startIndex:%d, self.buffer len:%d",
    start,
    endi,
    #result,
    self.startIndex,
    self:bufferSize()
  )
  -- self.logger.trace("fill>result:%s", util.table_tostring(result))

  return self.startIndex
end

-- trimming the buffer [fromIndex, end)
function LogBuffer:trim(fromIndex)
  if fromIndex < self:lastIndex() + 1 then
    for i = fromIndex, self:lastIndex() do
      self.buffer[i] = nil
      self.entriesInBuffer = self.entriesInBuffer - 1
    end
  end
end

function LogBuffer:append(entry)
  self.buffer[self:lastIndex() + 1] = entry
  self.entriesInBuffer = self.entriesInBuffer + 1
  -- self.logger.trace("append>index:%d->%s, self.startIndex:%d, self.buffer len:%d",
  --                    self:lastIndex(), util.table_tostring(entry), self.startIndex, self:bufferSize())
  -- maxSize
  if self.maxSize < self.entriesInBuffer then
    self.buffer[self.startIndex] = nil
    self.startIndex = self.startIndex + 1
    self.entriesInBuffer = self.entriesInBuffer - 1
  end
end

function LogBuffer:reset(startIndex)
  -- previous
  -- self.buffer = {}
  -- self.startIndex = startIndex
  -- self.entriesInBuffer = 0

  if startIndex > self.startIndex then
    for i = self.startIndex, startIndex - 1 do
      self.buffer[i] = nil
      self.entriesInBuffer = self.entriesInBuffer - 1
    end
  end
  self.startIndex = startIndex
end

function LogBuffer:bufferSize()
  return self.entriesInBuffer
end
