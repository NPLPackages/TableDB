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
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)script/Raft/LogEntry.lua");
local LogEntry = commonlib.gettable("Raft.LogEntry");
NPL.load("(gl)script/Raft/LogBuffer.lua");
local LogBuffer = commonlib.gettable("Raft.LogBuffer");

local LoggerFactory = NPL.load("(gl)script/Raft/LoggerFactory.lua");

local SequentialLogStore = commonlib.gettable("Raft.SequentialLogStore");


local LOG_INDEX_FILE = "store.idx";
local LOG_STORE_FILE = "store.data";
local LOG_START_INDEX_FILE = "store.sti";
local LOG_INDEX_FILE_BAK = "store.idx.bak";
local LOG_STORE_FILE_BAK = "store.data.bak";
local LOG_START_INDEX_FILE_BAK = "store.sti.bak";
local BUFFER_SIZE = 1000;

local UIntBytes = 4; -- 32 bits


function SequentialLogStore:new(logContainer) 
    local o = {
        logContainer = logContainer,
        logger = LoggerFactory.getLogger("SequentialLogStore"),
        zeroEntry = LogEntry:new(),
        bufferSize = BUFFER_SIZE,

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

    o.entriesInStore = o.indexFile:GetFileSize() / UIntBytes;

    o.buffer = LogBuffer:new((o.entriesInStore > o.bufferSize and (o.entriesInStore + o.startIndex - o.bufferSize)) or o.startIndex, o.bufferSize);

    o.logger.debug("log store started with entriesInStore=%d, startIndex=%d", o.entriesInStore, o.startIndex);

    return o;
end

function SequentialLogStore:__index(name)
    return rawget(self, name) or SequentialLogStore[name];
end

function SequentialLogStore:__tostring()
    return util.table_tostring(self)
end

--[[
   The first available index of the store, starts with 1
   @return value >= 1
]]--
function SequentialLogStore:getFirstAvailableIndex()
    return self.entriesInStore + self.startIndex;
end

--[[
   The start index of the log store, at the very beginning, it must be 1
   however, after some compact actions, this could be anything greater or equals to one
   @return start index of the log store
]]--
function SequentialLogStore:getStartIndex()
    return self.startIndex
end

--[[
   The last log entry in store
   @return a dummy constant entry with value set to null and term set to zero if no log entry in store
]]--
function SequentialLogStore:getLastLogEntry()
    local lastEntry = self.buffer:lastEntry();
    return (lastEntry == nil and self.zeroEntry) or lastEntry;
end

--[[
   Appends a log entry to store
   @param logEntry
   @return the last appended log index
]]--
function SequentialLogStore:append(logEntry)
    self.indexFile:seek(self.indexFile:GetFileSize());
    local dataFileLength = self.dataFile:GetFileSize();
    self.indexFile:WriteUInt(dataFileLength);
    self.dataFile:seek(dataFileLength);
    self.dataFile:WriteUInt(logEntry.term);
    self.dataFile:WriteBytes(1, {logEntry.valueType});
    self.dataFile:WriteBytes(#logEntry.value, {logEntry.value:byte(1, -1)});

    self.entriesInStore = self.entriesInStore + 1;
    self.buffer:append(logEntry);
    return self.entriesInStore + self.startIndex - 1;
end

--[[
   Over writes a log entry at index of {@code index}
   @param index a value < {@code this.getFirstAvailableIndex()}, and starts from 1
   @param logEntry
]]--
function SequentialLogStore:writeAt(logIndex, logEntry)
    if logIndex < self.startIndex then
        return;
    end

    local index = logIndex - self.startIndex + 1
    -- find the positions for index and data files
    local dataPosition = self.dataFile:GetFileSize();
    local indexPosition = (index - 1) * UIntBytes;
    if(indexPosition < self.indexFile:GetFileSize()) then
        self.indexFile:seek(indexPosition);
        dataPosition = self.indexFile:ReadUInt();
    end

    -- write the data at the specified position
    self.indexFile:seek(indexPosition);
    self.dataFile:seek(dataPosition);
    self.indexFile:WriteUInt(dataPosition);
    self.dataFile:WriteUInt(logEntry.term);
    self.dataFile:WriteBytes(1, {logEntry.valueType});
    self.dataFile:WriteBytes(#logEntry.value, {logEntry.value:byte(1, -1)});


    -- trim the files if necessary
    if(self.indexFile:GetFileSize() > self.indexFile:getpos()) then
        self.indexFile:SetEndOfFile();
    end

    if(self.dataFile:GetFileSize() > self.dataFile:getpos()) then
        self.dataFile:SetEndOfFile();
    end

    if(index <= self.entriesInStore) then
        self.buffer:trim(logIndex);
    end
    
    self.buffer:append(logEntry);
    self.entriesInStore = index;
end

--[[
   Get log entries with index between {@code start} and {@code end}
   @param start the start index of log entries
   @param end the end index of log entries (exclusive)
   @return the log entries between [start, end)
]]--
function SequentialLogStore:getLogEntries(startIndex, endIndex)
    self.logger.trace("getLogEntries:startIndex:%d, endIndex:%d, self.startIndex:%d, self.entriesInStore:%d",
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
    self.logger.trace("getLogEntries:bufferFirstIndex:%d, got entries len:%d", bufferFirstIndex, #entries)
    
    -- Assumption: buffer.lastIndex() == this.entriesInStore + this.startIndex
    -- (Yes, for sure, we need to enforce this assumption to be true)
    if(startIndex < bufferFirstIndex) then
        -- in this case, we need to read from store file
        local endi = bufferFirstIndex - self.startIndex;
        self.indexFile:seek(start * UIntBytes);
        local dataStart = self.indexFile:ReadUInt();
        for i = 1, (endi - start) do
            local dataEnd = self.indexFile:ReadUInt();
            local dataSize = dataEnd - dataStart;
            self.dataFile:seek(dataStart);
            -- here we should use i to index
            entries[i] = self:readEntry(dataSize);
            dataStart = dataEnd;
        end
    end
    return entries;
end

--[[
   Gets the log entry at the specified index
   @param index starts from 1
   @return the log entry or null if index >= {@code this.getFirstAvailableIndex()}
]]--
function SequentialLogStore:getLogEntryAt(logIndex)
    self.logger.trace("SequentialLogStore:getLogEntryAt>logIndex:%d, self.startIndex:%d, self.entriesInStore:%d",
                       logIndex, self.startIndex, self.entriesInStore)
    if logIndex < self.startIndex then
        return;
    end

    local index = logIndex - self.startIndex + 1;
    if(index > self.entriesInStore) then
        return;
    end

    local entry = self.buffer:entryAt(logIndex);
    if(entry ~= nil) then
        return entry;
    end

    local indexPosition = (index - 1) * UIntBytes;
    self.indexFile:seek(indexPosition);
    local dataPosition = self.indexFile:ReadUInt();
    local endDataPosition = self.indexFile:ReadUInt();
    self.dataFile:seek(dataPosition);
    return self:readEntry(endDataPosition - dataPosition)
 end

--[[
   Pack {@code itemsToPack} log items starts from {@code index}
   @param index
   @param itemsToPack
   @return log pack
]]--
function SequentialLogStore:packLog(index, itemsToPack)
end

--[[
   Apply the log pack to current log store, starting from index
   @param index the log index that start applying the logPack, index starts from 1
   @param logPack
]]--
function SequentialLogStore:applyLogPack(index, logPack)
end

--[[
   Compact the log store by removing all log entries including the log at the lastLogIndex
   @param lastLogIndex
   @return compact successfully or not
]]--
function SequentialLogStore:compact(lastLogIndex)
end

function SequentialLogStore:fillBuffer()
    local startIndex = self.buffer:firstIndex();
    local indexFileSize = self.indexFile:GetFileSize();
    if(indexFileSize > 0) then
        local indexPosition = (startIndex - self.startIndex) * UIntBytes;
        self.indexFile:seek(indexPosition);
        local indexData = {};
        self.indexFile:read(indexFileSize - indexPosition, indexData);
        
        -- convert bytes to UInt...
        local dataStart = self:readUInt(indexData);
        self.dataFile:seek(dataStart);
        while(#indexData > 0) do
            local dataEnd = self:readUInt(indexData);
            self.buffer:append(self:readEntry(dataEnd - dataStart));
            dataStart = dataEnd;
        end
    end
end

function SequentialLogStore:readEntry(size)
    local term = self.dataFile:ReadUInt();
    local value = {}
    self.dataFile:ReadBytes(size-UIntBytes, value);
    local valueType = value[1]
    value[1] = nil
    return LogEntry:new(term, value, valueType);
end


-- assume little endian and unsigned
function SequentialLogStore:readUInt(t)
    local n = 0
    for k=1,4 do
        n = n + t[k]*2^((k-1)*8)
        -- set nil
        t[k] = nil
    end
    return n;
end

local function bytes_to_int(str,endian,signed) -- use length of string to determine 8,16,32,64 bits
    local t={str:byte(1,-1)}
    if endian=="big" then --reverse bytes
        local tt={}
        for k=1,#t do
            tt[#t-k+1]=t[k]
        end
        t=tt
    end
    local n=0
    for k=1,#t do
        n=n+t[k]*2^((k-1)*8)
    end
    if signed then
        n = (n > 2^(#t-1) -1) and (n - 2^#t) or n -- if last bit set, negative.
    end
    return n
end
