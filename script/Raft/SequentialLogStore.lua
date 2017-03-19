--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/SequentialLogStore.lua");
local SequentialLogStore = commonlib.gettable("Raft.SequentialLogStore");
------------------------------------------------------------
]]--

local LoggerFactory = NPL.load("(gl)script/Raft/LoggerFactory.lua");

local SequentialLogStore = commonlib.gettable("Raft.SequentialLogStore");


local LOG_INDEX_FILE = "store.idx";
local LOG_STORE_FILE = "store.data";
local LOG_START_INDEX_FILE = "store.sti";
local LOG_INDEX_FILE_BAK = "store.idx.bak";
local LOG_STORE_FILE_BAK = "store.data.bak";
local LOG_START_INDEX_FILE_BAK = "store.sti.bak";

function SequentialLogStore:new(logContainer) 
    local o = {
        logContainer = logContainer,
        logger = LoggerFactory.getLogger("SequentialLogStore"),
    };
    setmetatable(o, self);


    -- index file
    local indexFileName = o.logContainer..LOG_INDEX_FILE
    if not ParaIO.DoesFileExist(indexFileName) then
        local result = ParaIO.CreateNewFile(indexFileName)
        assert(result, "create indexFile failed")
    end
    o.indexFile = ParaIO.open(indexFileName, "rw");
    assert(o.indexFile:IsValid(), "indexFile not Valid")

    -- data file
    local dataFileName = o.logContainer..LOG_STORE_FILE
    if not ParaIO.DoesFileExist(dataFileName) then
        local result = ParaIO.CreateNewFile(dataFileName)
        assert(result, "create dataFile failed")
    end
    o.dataFile = ParaIO.open(dataFileName, "rw");
    assert(o.dataFile:IsValid(), "dataFile not Valid")

    -- startIndex file
    local startIndexFileName = o.logContainer..LOG_START_INDEX_FILE
    if not ParaIO.DoesFileExist(startIndexFileName) then
        local result = ParaIO.CreateNewFile(startIndexFileName)
        assert(result, "create startIndexFile failed")
    end
    o.startIndexFile = ParaIO.open(startIndexFileName, "rw");
    assert(o.startIndexFile:IsValid(), "startIndexFile not Valid")

    if(o.startIndexFile:GetFileSize() == 0) then
        o.startIndex = 1;
        o.startIndexFile:WriteUInt(o.startIndex);
    else
        o.startIndex = o.startIndexFile:ReadUInt();
    end

    local UintBytes = 4; -- 32 bits
    o.entriesInStore = o.indexFile:GetFileSize() / UintBytes;

    o.logger.debug(string.format("log store started with entriesInStore=%d, startIndex=%d", o.entriesInStore, o.startIndex));

    return o;
end

function SequentialLogStore:__index(name)
    return rawget(self, name) or SequentialLogStore[name];
end

function SequentialLogStore:__tostring()
    return util.table_tostring(self)
end

--[[
 * The first available index of the store, starts with 1
 * @return value >= 1
]]--
function SequentialLogStore:getFirstAvailableIndex()
    return self.entriesInStore + self.startIndex;
end

--[[
 * The start index of the log store, at the very beginning, it must be 1
 * however, after some compact actions, this could be anything greater or equals to one
 * @return start index of the log store
]]--
function SequentialLogStore:getStartIndex()
    return self.startIndex
end

--[[
 * The last log entry in store
 * @return a dummy constant entry with value set to null and term set to zero if no log entry in store
]]--
function SequentialLogStore:getLastLogEntry()
end

--[[
 * Appends a log entry to store
 * @param logEntry
 * @return the last appended log index
]]--
function SequentialLogStore:append(logEntry)
end

--[[
 * Over writes a log entry at index of {@code index}
 * @param index a value < {@code this.getFirstAvailableIndex()}, and starts from 1
 * @param logEntry
]]--
function SequentialLogStore:writeAt(index, logEntry)
end

--[[
 * Get log entries with index between {@code start} and {@code end}
 * @param start the start index of log entries
 * @param end the end index of log entries (exclusive)
 * @return the log entries between [start, end)
]]--
function SequentialLogStore:getLogEntries(start, endi)
end

--[[
 * Gets the log entry at the specified index
 * @param index starts from 1
 * @return the log entry or null if index >= {@code this.getFirstAvailableIndex()}
]]--
function SequentialLogStore:getLogEntryAt(index)
end

--[[
 * Pack {@code itemsToPack} log items starts from {@code index}
 * @param index
 * @param itemsToPack
 * @return log pack
]]--
function SequentialLogStore:packLog(index, itemsToPack)
end

--[[
 * Apply the log pack to current log store, starting from index
 * @param index the log index that start applying the logPack, index starts from 1
 * @param logPack
]]--
function SequentialLogStore:applyLogPack(index, logPack)
end

--[[
 * Compact the log store by removing all log entries including the log at the lastLogIndex
 * @param lastLogIndex
 * @return compact successfully or not
]]--
function SequentialLogStore:compact(lastLogIndex)
end