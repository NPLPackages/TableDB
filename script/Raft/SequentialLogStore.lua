--[[
Title: 
Author: liuluheng
Date: 2017.03.25
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

-- NOTE: 
-- we use Double instead of Long at where it should be a Long
-- but is the conversion between double and bytes is correct?
-- it should be
local DoubleBytes = 8; -- 64 bits


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
    o.indexFile = ParaIO.open(indexFileName, "rw");
    assert(o.indexFile:IsValid(), "indexFile not Valid")

    -- data file
    local dataFileName = o.logContainer..LOG_STORE_FILE
    o.dataFile = ParaIO.open(dataFileName, "rw");
    assert(o.dataFile:IsValid(), "dataFile not Valid")

    -- startIndex file
    local startIndexFileName = o.logContainer..LOG_START_INDEX_FILE
    o.startIndexFile = ParaIO.open(startIndexFileName, "rw");
    assert(o.startIndexFile:IsValid(), "startIndexFile not Valid")

    if(o.startIndexFile:GetFileSize() == 0) then
        o.startIndex = 1;
        o.startIndexFile:WriteDouble(o.startIndex);
    else
        o.startIndex = o.startIndexFile:ReadDouble();
    end

    o.entriesInStore = o.indexFile:GetFileSize() / DoubleBytes;

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
    self.indexFile:WriteDouble(dataFileLength);
    self.dataFile:seek(dataFileLength);
    self.dataFile:WriteDouble(logEntry.term);
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
    local indexPosition = (index - 1) * DoubleBytes;
    if(indexPosition < self.indexFile:GetFileSize()) then
        self.indexFile:seek(indexPosition);
        dataPosition = self.indexFile:ReadDouble();
    end

    -- write the data at the specified position
    self.indexFile:seek(indexPosition);
    self.dataFile:seek(dataPosition);
    self.indexFile:WriteDouble(dataPosition);
    self.dataFile:WriteDouble(logEntry.term);
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
        self.indexFile:seek(start * DoubleBytes);
        local dataStart = self.indexFile:ReadDouble();
        for i = 1, (endi - start) do
            local dataEnd = self.indexFile:ReadDouble();
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

    local indexPosition = (index - 1) * DoubleBytes;
    self.indexFile:seek(indexPosition);
    local dataPosition = self.indexFile:ReadDouble();
    local endDataPosition = self.indexFile:ReadDouble();
    self.dataFile:seek(dataPosition);
    return self:readEntry(endDataPosition - dataPosition)
 end

--[[
   Pack {@code itemsToPack} log items starts from {@code index}
   @param index
   @param itemsToPack
   @return log pack
]]--
function SequentialLogStore:packLog(logIndex, itemsToPack)
    if logIndex < self.startIndex then
        return;
    end

    local index = logIndex - self.startIndex + 1;
    if(index > self.entriesInStore) then
        return {};
    end

    local endIndex = math.min(index + itemsToPack, self.entriesInStore + 1);
    local readToEnd = (endIndex == self.entriesInStore + 1);
    local indexPosition = (index - 1) * DoubleBytes;
    self.indexFile:seek(indexPosition);

    local endOfLog = self.dataFile:GetFileSize();
    local indexBytes = DoubleBytes * (endIndex - index)
    if(not readToEnd) then
        self.indexFile:seek(indexBytes)
        endOfLog = self.indexFile:ReadDouble();
    end

    local startOfLog = self.indexFile:ReadDouble();
    self.dataFile:seek(startOfLog);

    -- "<memory>" is a special name for memory file, both read/write is possible. 
    local file = ParaIO.open("<memory>", "w");
    local bytes;
    if(file:IsValid()) then
        local dataBytes = endOfLog - startOfLog
        file:WriteDouble(indexBytes)
        file:WriteDouble(dataBytes)
       
        -- index data
        file:WriteBytes(indexBytes, {self.indexFile:GetText(indexPosition, -1):byte(1, -1)})
        file:WriteBytes(dataBytes, {self.dataFile:GetText(startOfLog, -1):byte(1, -1)})
        bytes = file:GetText(0, -1)
        file:close()
	 
        -- Compress
        local data = {content=bytes, method="gzip"};
        if(NPL.Compress(data)) then
            bytes = data.result;
        end
    end
    return bytes;

    -- error handle
    -- self.logger.error("failed to read files to read data for packing");
end

--[[
   Apply the log pack to current log store, starting from index
   @param index the log index that start applying the logPack, index starts from 1
   @param logPack
]]--
function SequentialLogStore:applyLogPack(logIndex, logPack)
    if logIndex < self.startIndex then
        return;
    end

    local index = logIndex - self.startIndex + 1;

    local bytes;
    local data = {content=logPack, method="gzip"};
    if(NPL.Decompress(data)) then
        bytes = data.result;
    end

    -- "<memory>" is a special name for memory file, both read/write is possible. 
    local file = ParaIO.open("<memory>", "w");
    local bytes;
    if(file:IsValid()) then
        file:WriteBytes(#bytes, {bytes:byte(1, -1)})
        file:seek(0)

        local indexBytes = file:ReadDouble()
        local dataBytes = file:ReadDouble()

        local indexBuffer = {}
        local logBuffer = {}
       
        -- index data
        file:ReadBytes(indexBytes, indexBuffer)
        file:ReadBytes(dataBytes, logBuffer)

        local indexFilePosition, dataFilePosition;
        if(index == self.entriesInStore + 1) then
            indexFilePosition = self.indexFile:GetFileSize();
            dataFilePosition = self.dataFile:GetFileSize();
        else
            indexFilePosition = (index - 1) * DoubleBytes;
            self.indexFile.seek(indexFilePosition);
            dataFilePosition = self.indexFile:ReadDouble();
        end


        self.indexFile:seek(indexFilePosition);
        self.indexFile:WriteBytes(indexBytes, indexBuffer);
        self.indexFile:SetEndOfFile();
        self.dataFile:seek(dataFilePosition);
        self.dataFile:WriteBytes(dataBytes, logBuffer);
        self.dataFile:SetEndOfFile();
        self.entriesInStore = index - 1 + indexBytes / DoubleBytes;

        self.buffer:reset(self.entriesInStore > self.bufferSize and self.entriesInStore + self.startIndex - self.bufferSize or self.startIndex);
        self:fillBuffer();
        file:close()
    end

    -- error handle
    -- self.logger.error("failed to write files to unpack logs for data");
end

--[[
   Compact the log store by removing all log entries including the log at the lastLogIndex
   @param lastLogIndex
   @return compact successfully or not
]]--
function SequentialLogStore:compact(lastLogIndex)
    if lastLogIndex < self.startIndex then
        return;
    end

    self:backup();
    local lastIndex = lastLogIndex - self.startIndex;
    if(lastLogIndex >= self:getFirstAvailableIndex() - 1) then
        self.indexFile:seek(0)
        self.indexFile:SetEndOfFile();
        self.dataFile:seek(0);
        self.dataFile:SetEndOfFile();
        self.startIndexFile:seek(0);
        self.startIndexFile:WriteDouble(lastLogIndex + 1);
        self.startIndex = lastLogIndex + 1;
        self.entriesInStore = 0;
        self.buffer:reset(lastLogIndex + 1);
        return true;
    else
        local dataPosition = -1;
        local indexPosition = DoubleBytes * (lastIndex + 1);
        self.indexFile:seek(indexPosition);
        dataPosition = self.indexFile:ReadDouble()
        local indexFileNewLength = self.indexFile:GetFileSize() - indexPosition;
        local dataFileNewLength = self.dataFile:GetFileSize() - dataPosition;

        -- copy the log data
        -- data file
        local backupDataFileName = o.logContainer..LOG_STORE_FILE_BAK
        local backupFile = ParaIO.open(backupDataFileName, "r");
        assert(backupFile:IsValid(), "dataFile not Valid")

        -- we don't have an channel, so this is inefficient and ugly
        backupFile:seek(dataPosition);
        local data = {}
        backupFile:ReadBytes(dataFileNewLength, data)
        self.dataFile:seek(0)
        self.dataFile:WriteBytes(dataFileNewLength, data)
        self.dataFile:SetEndOfFile()
        backupFile:close();

        -- copy the index data
        -- index file
        local backupIndexFileName = o.logContainer..LOG_INDEX_FILE_BAK
        backupFile = ParaIO.open(backupIndexFileName, "r");
        assert(backupFile:IsValid(), "backupFile not Valid")

        
        backupFile = new RandomAccessFile(self.logContainer.resolve(LOG_INDEX_FILE_BAK).toString(), "r");
        backupFile:seek(indexPosition);
        self.indexFile:seek(0);
        for  i = 1, indexFileNewLength / DoubleBytes do
            self.indexFile:WriteDouble(backupFile:ReadDouble() - dataPosition);
        end

        self.indexFile:SetEndOfFile();
        backupFile:close();

        -- save the starting index
        self.startIndexFile:seek(0);
        self.startIndexFile:WriteDouble(lastLogIndex + 1);
        self.entriesInStore = self.entriesInStore - (lastLogIndex - self.startIndex + 1);
        self.startIndex = lastLogIndex + 1;
        self.buffer.reset(self.entriesInStore > self.bufferSize and self.entriesInStore + self.startIndex - self.bufferSize or self.startIndex);
        self:fillBuffer();
        return true;
    end

    self.logger.error("fail to compact the logs due to error");
    self:restore();
    return false;
end

function SequentialLogStore:fillBuffer()
    local startIndex = self.buffer:firstIndex();
    local indexFileSize = self.indexFile:GetFileSize();
    if(indexFileSize > 0) then
        local indexPosition = (startIndex - self.startIndex) * DoubleBytes;
        self.indexFile:seek(indexPosition);        
        local dataStart = self.indexFile:readDouble(indexData);
        self.dataFile:seek(dataStart);
        while(self.indexFile:getpos() < indexFileSize) do
            local dataEnd = self:readDouble(indexData);
            self.buffer:append(self:readEntry(dataEnd - dataStart));
            dataStart = dataEnd;
        end
    end
end


function SequentialLogStore:restore()
    self.indexFile:close();
    self.dataFile:close();
    self.startIndexFile:close();
    local indexFileName = self.logContainer..LOG_INDEX_FILE
    local dataFileName = self.logContainer..LOG_STORE_FILE
    local startIndexFileName = self.logContainer..LOG_START_INDEX_FILE
    
    local backupIndexFileName = self.logContainer..LOG_INDEX_FILE_BAK
    local backupDataFileName = self.logContainer..LOG_STORE_FILE_BAK
    local backupStartIndexFileName = self.logContainer..LOG_START_INDEX_FILE_BAK

    if (ParaIO.CopyFile(backupIndexFileName, indexFileName, true) and
        ParaIO.CopyFile(backupDataFileName, dataFileName, true) and
        ParaIO.CopyFile(backupStartIndexFileName, startIndexFileName, true)) then

        self.indexFile = ParaIO.open(indexFileName, "rw");
        self.dataFile = ParaIO.open(dataFileName, "rw");
        self.startIndexFile = ParaIO.open(startIndexFileName, "rw");
    
    else
        -- this is fatal...
        self.logger.fatal("cannot restore from failure, please manually restore the log files");
    end

end

function SequentialLogStore:backup()
    --decide not to use ParaIO.BackupFile
    local indexFileName = self.logContainer..LOG_INDEX_FILE
    local dataFileName = self.logContainer..LOG_STORE_FILE
    local startIndexFileName = self.logContainer..LOG_START_INDEX_FILE
    
    local backupIndexFileName = self.logContainer..LOG_INDEX_FILE_BAK
    local backupDataFileName = self.logContainer..LOG_STORE_FILE_BAK
    local backupStartIndexFileName = self.logContainer..LOG_START_INDEX_FILE_BAK

    ParaIO.DeleteFile(backupDataFileName)
    ParaIO.DeleteFile(backupIndexFileName)
    ParaIO.DeleteFile(backupStartIndexFileName)

    if not (ParaIO.CopyFile(indexFileName, backupIndexFileName, true) and
        ParaIO.CopyFile(dataFileName, backupDataFileName, true) and
        ParaIO.CopyFile(startIndexFileName, backupStartIndexFileName, true)) then
        self.logger.error("failed to create a backup folder")
    end
end


function SequentialLogStore:readEntry(size)
    local term = self.dataFile:ReadDouble();
    local value = {}
    self.dataFile:ReadBytes(size-DoubleBytes, value);
    local valueType = value[1]
    value[1] = nil
    return LogEntry:new(term, value, valueType);
end


-- assume little endian and unsigned
-- function SequentialLogStore:readUInt(t)
--     local n = 0
--     for k=1,UIntBytes do
--         n = n + t[k]*2^((k-1)*8)
--         -- set nil
--         t[k] = nil
--     end
--     return n;
-- end

-- local function bytes_to_int(str,endian,signed) -- use length of string to determine 8,16,32,64 bits
--     local t={str:byte(1,-1)}
--     if endian=="big" then --reverse bytes
--         local tt={}
--         for k=1,#t do
--             tt[#t-k+1]=t[k]
--         end
--         t=tt
--     end
--     local n=0
--     for k=1,#t do
--         n=n+t[k]*2^((k-1)*8)
--     end
--     if signed then
--         n = (n > 2^(#t-1) -1) and (n - 2^#t) or n -- if last bit set, negative.
--     end
--     return n
-- end
