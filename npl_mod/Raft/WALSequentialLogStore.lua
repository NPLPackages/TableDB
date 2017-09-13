--[[
Title:
Author: liuluheng
Date: 2017.03.25
Desc:
not touch the disk.
------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/WALSequentialLogStore.lua");
local WALSequentialLogStore = commonlib.gettable("Raft.WALSequentialLogStore");
------------------------------------------------------------
]]
--
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)npl_mod/Raft/LogEntry.lua");
local LogEntry = commonlib.gettable("Raft.LogEntry");
NPL.load("(gl)npl_mod/Raft/WALLogBuffer.lua");
local WALLogBuffer = commonlib.gettable("Raft.WALLogBuffer");
NPL.load("(gl)script/ide/System/Database/TableDatabase.lua");
local TableDatabase = commonlib.gettable("System.Database.TableDatabase");

local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua");
local LogValueType = NPL.load("(gl)npl_mod/Raft/LogValueType.lua");

local WALSequentialLogStore = commonlib.gettable("Raft.WALSequentialLogStore");

local BUFFER_SIZE = 1000;
local LOG_START_INDEX_FILE = "store.sti";


function WALSequentialLogStore:new(logContainer)
    local o = {
        logContainer = logContainer,
        logger = LoggerFactory.getLogger("WALSequentialLogStore"),
        zeroEntry = LogEntry:new(),
        -- actually not used
        bufferSize = BUFFER_SIZE,
        -- this will start both db client and db server if not.
        db = TableDatabase:new():connect(logContainer, function() end);
    };
    setmetatable(o, self);
    
    o.startIndexFileName = o.logContainer..LOG_START_INDEX_FILE
    o.startIndexFile = ParaIO.open(o.startIndexFileName, "rw");
    assert(o.startIndexFile:IsValid(), "startIndex cannot open");
    local startIndexFileSize = o.startIndexFile:GetFileSize()
    if(startIndexFileSize == 0) then
        o.startIndex = 1;
        o.startIndexFile:WriteDouble(o.startIndex);
    else
        o.startIndex = o.startIndexFile:ReadDouble();
    end

    o.entriesInStore = 0;
    o.buffer = WALLogBuffer:new(o.startIndex, o.bufferSize);
    
    return o;
end

function WALSequentialLogStore:__index(name)
    return rawget(self, name) or WALSequentialLogStore[name];
end

function WALSequentialLogStore:__tostring()
    return util.table_tostring(self)
end

--[[
The first available index of the store, starts with 1
@return value >= 1
]]
--
function WALSequentialLogStore:getFirstAvailableIndex()
    return self.entriesInStore + self.startIndex;
end

--[[
The start index of the log store, at the very beginning, it must be 1
however, after some compact actions, this could be anything greater or equals to one
@return start index of the log store
]]
--
function WALSequentialLogStore:getStartIndex()
    return self.startIndex
end

--[[
The last log entry in store
@return a dummy constant entry with value set to null and term set to zero if no log entry in store
]]
--
function WALSequentialLogStore:getLastLogEntry()
    local lastEntry = self.buffer:lastEntry();
    return (lastEntry == nil and self.zeroEntry) or lastEntry;
end

--[[
Appends a log entry to store
@param logEntry
@return the last appended log index
]]
--
function WALSequentialLogStore:append(logEntry)
    self.entriesInStore = self.entriesInStore + 1;
    self.buffer:append(logEntry);
    
    local logIndex = self.entriesInStore + self.startIndex - 1;
    if logEntry.valueType ~= LogValueType.Application then
        -- use TableDB deal with LogValueType other than Application
        self.db.raftLog:insertOne(nil, {logIndex = logIndex, logEntry = logEntry, });
    end
    
    return logIndex;
end

--[[
Over writes a log entry at index of {@code index}
@param index a value < {@code this.getFirstAvailableIndex()}, and starts from 1
@param logEntry
]]
--
function WALSequentialLogStore:writeAt(logIndex, logEntry)
    if logIndex < self.startIndex then
        return;
    end
    
    local index = logIndex - self.startIndex
    if (index <= self.entriesInStore) then
        self.buffer:trim(logIndex);
        
        if logEntry.valueType ~= LogValueType.Application then
            -- use TableDB deal with LogValueType other than Application
            self.db.raftLog:find({_id = {gt = logIndex}}, function(err, data)
                if not err and data then
                    for _, v in ipairs(data) do
                        self.db.raftLog:deleteOne({logIndex = data.logIndex});
                    end
                end
            end);
            self.db.raftLog:insertOne({logIndex = logIndex}, {logIndex = logIndex, logEntry = logEntry, });
        end
    end
    
    self.buffer:append(logEntry);
    self.entriesInStore = index;
end

--[[
Get log entries with index between {@code start} and {@code end}
@param start the start index of log entries
@param end the end index of log entries (exclusive)
@return the log entries between [start, end)
]]
--
function WALSequentialLogStore:getLogEntries(startIndex, endIndex)
    self.logger.trace("getLogEntries:startIndex:%d, endIndex:%d, startIndex:%d, entriesInStore:%d",
        startIndex, endIndex, self.startIndex, self.entriesInStore)
    if startIndex < self.startIndex then
        return;
    end
    
    -- start and adjustedEnd are zero based, targetEndIndex is this.startIndex based
    local start = startIndex - self.startIndex;
    local adjustedEnd = endIndex - self.startIndex;
    adjustedEnd = (adjustedEnd > self.entriesInStore and self.entriesInStore) or adjustedEnd;
    local targetEndIndex = (endIndex > self.entriesInStore + self.startIndex + 1 and self.entriesInStore + self.startIndex + 1) or endIndex;
    
    local entries = {}
    if adjustedEnd - start == 0 then
        return entries
    end
    
    self.logger.trace("getLogEntries:pre fill entries len:%d", #entries)
    -- fill with buffer
    local bufferFirstIndex = self.buffer:fill(startIndex, targetEndIndex, entries);
    
    -- Assumption: buffer.lastIndex() == this.entriesInStore + this.startIndex
    -- (Yes, for sure, we need to enforce this assumption to be true)
    if (startIndex < bufferFirstIndex) then
        -- should never goes here
        self.logger.error("badly wrong!! getting entries not in the buffer")
    end
    
    return entries;
end

--[[
Gets the log entry at the specified index
@param index starts from 1
@return the log entry or null if index >= {@code this.getFirstAvailableIndex()}
]]
--
function WALSequentialLogStore:getLogEntryAt(logIndex)
    self.logger.trace("getLogEntryAt>logIndex:%d, startIndex:%d, entriesInStore:%d",
        logIndex, self.startIndex, self.entriesInStore)
    if logIndex < self.startIndex then
        return;
    end
    
    local index = logIndex - self.startIndex + 1;
    if (index > self.entriesInStore) then
        return;
    end
    
    local entry = self.buffer:entryAt(logIndex);
    if (entry ~= nil) then
        return entry;
    else
        self.db:EnableSyncMode(true);

        local err, data = self.db.raftLog:findOne({logIndex = logIndex});
        if not err and data and data.logEntry then
            entry = data.logEntry
        else
            self.logger.error("badly wrong!! getting %d entry not in the raftLog DB", logIndex);
        end
        self.db:EnableSyncMode(false);
    end
    
    return entry;
end

--[[
Pack {@code itemsToPack} log items starts from {@code index}
@param index
@param itemsToPack
@return log pack
]]
--
function WALSequentialLogStore:packLog(logIndex, itemsToPack)
    self.logger.trace("packLog>logIndex:%d, itemsToPack:%d, startIndex:%d, entriesInStore:%d",
        logIndex, itemsToPack, self.startIndex, self.entriesInStore);
    if logIndex < self.startIndex then
        return;
    end
    
    local index = logIndex - self.startIndex + 1;
    if (index > self.entriesInStore) then
        return {};
    end
    
    local buffer = {};
    for i = 1, itemsToPack do
        buffer[#buffer + 1] = self.buffer:entryAt(logIndex + i - 1);
    end
    local str = commonlib.serialize_compact2(buffer)
    local file = ParaIO.open("<memory>", "w");
    local bytes;
    if (file:IsValid()) then
        file:WriteInt(itemsToPack);
        file:WriteInt(#str)
        file:WriteString(str)
        bytes = file:GetText(0, -1)
        file:close()
        -- Compress
        local data = {content = bytes, method = "gzip"};
        if (NPL.Compress(data)) then
            bytes = data.result;
        end
    end
    
    return bytes;
end

--[[
Apply the log pack to current log store, starting from index
@param index the log index that start applying the logPack, index starts from 1
@param logPack
]]
--
function WALSequentialLogStore:applyLogPack(logIndex, logPack)
    self.logger.trace("applyLogPack>logIndex:%d, startIndex:%d, entriesInStore:%d",
        logIndex, self.startIndex, self.entriesInStore);
    if logIndex < self.startIndex then
        return;
    end
    
    local bytes;
    local data = {content = logPack, method = "gzip"};
    if (NPL.Decompress(data)) then
        bytes = data.result;
    end
    local file = ParaIO.open("<memory>", "w");
    if (file:IsValid()) then
        if type(bytes) == "string" then
            file:write(bytes, #bytes);
        elseif type(bytes) == "table" then
            file:WriteBytes(#bytes, bytes);
        end
        file:seek(0)
        
        local items = file:ReadInt();
        local index = logIndex - self.startIndex + 1;
        if(index == self.entriesInStore + 1) then
            self.entriesInStore = index - 1 + items;
        end
        
        local n = file:ReadInt();
        local str = file:ReadString(n)
        -- print(str)
        local buffer = commonlib.LoadTableFromString(str);
        
        for i, v in ipairs(buffer) do
            self.buffer:writeAt(logIndex + i - 1, v);
        end
    end

-- self.buffer:reset(self.startIndex);
end

--[[
Compact the log store by removing all log entries including the log at the lastLogIndex
@param lastLogIndex
@return compact successfully or not
]]
--
function WALSequentialLogStore:compact(lastLogIndex)
    if lastLogIndex < self.startIndex then
        return;
    end
    
    if (lastLogIndex >= self:getFirstAvailableIndex() - 1) then
        self.entriesInStore = 0;
    else
        self.entriesInStore = self.entriesInStore - (lastLogIndex - self.startIndex + 1);
    end

    self.startIndex = lastLogIndex + 1;
    self.buffer:reset(self.startIndex);
    -- save the starting index
    self.startIndexFile:seek(0);
    self.startIndexFile:WriteDouble(self.startIndex);
    
    return true;
end


function WALSequentialLogStore:readEntry(size)
-- need this?
-- read Entry from WAL
end

function WALSequentialLogStore:close()
-- self.db.raftLog:close();
    self.startIndexFile:close();
end