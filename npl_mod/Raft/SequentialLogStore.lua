--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 
  log interface in Raft

------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/SequentialLogStore.lua");
local SequentialLogStore = commonlib.gettable("Raft.SequentialLogStore");
------------------------------------------------------------
]] --
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua")
local util = commonlib.gettable("System.Compiler.lib.util")

NPL.load("(gl)npl_mod/Raft/LogBuffer.lua")
local LogBuffer = commonlib.gettable("Raft.LogBuffer")

local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua")

local SequentialLogStore = commonlib.gettable("Raft.SequentialLogStore")

local BUFFER_SIZE = 1000

function SequentialLogStore:new(logContainer)
  local o = {
    logContainer = logContainer,
    logger = LoggerFactory.getLogger("SequentialLogStore"),
    bufferSize = BUFFER_SIZE
  }
  setmetatable(o, self)

  return o
end

function SequentialLogStore:__index(name)
  return rawget(self, name) or SequentialLogStore[name]
end

function SequentialLogStore:__tostring()
  return util.table_tostring(self)
end
 --

--[[
   The first available index of the store, starts with 1
   @return value >= 1
]] function SequentialLogStore:getFirstAvailableIndex()
end
 --

--[[
   The start index of the log store, at the very beginning, it must be 1
   however, after some compact actions, this could be anything greater or equals to one
   @return start index of the log store
]] function SequentialLogStore:getStartIndex()
end
 --

--[[
   The last log entry in store
   @return a dummy constant entry with value set to null and term set to zero if no log entry in store
]] function SequentialLogStore:getLastLogEntry()
end
 --

--[[
   Appends a log entry to store
   @param logEntry
   @return the last appended log index
]] function SequentialLogStore:append(
  logEntry)
end
 --

--[[
   Over writes a log entry at index of {@code index}
   @param index a value < {@code this.getFirstAvailableIndex()}, and starts from 1
   @param logEntry
]] function SequentialLogStore:writeAt(
  logIndex,
  logEntry)
end
 --

--[[
   Get log entries with index between {@code start} and {@code end}
   @param start the start index of log entries
   @param end the end index of log entries (exclusive)
   @return the log entries between [start, end)
]] function SequentialLogStore:getLogEntries(
  startIndex,
  endIndex)
end
 --

--[[
   Gets the log entry at the specified index
   @param index starts from 1
   @return the log entry or null if index >= {@code this.getFirstAvailableIndex()}
]] function SequentialLogStore:getLogEntryAt(
  logIndex)
end
 --

--[[
   Pack {@code itemsToPack} log items starts from {@code index}
   @param index
   @param itemsToPack
   @return log pack
]] function SequentialLogStore:packLog(
  logIndex,
  itemsToPack)
end
 --

--[[
   Apply the log pack to current log store, starting from index
   @param index the log index that start applying the logPack, index starts from 1
   @param logPack
]] function SequentialLogStore:applyLogPack(
  logIndex,
  logPack)
end
 --

--[[
   Compact the log store by removing all log entries including the log at the lastLogIndex
   @param lastLogIndex
   @return compact successfully or not
]] function SequentialLogStore:compact(
  lastLogIndex)
end
