--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 

NPL file API dose not support u GetFileSize in "rw" mode(and variou things like this),
so the code is a bit ugly :(

------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/SequentialLogStore.lua");
local SequentialLogStore = commonlib.gettable("Raft.SequentialLogStore");
------------------------------------------------------------
]]--
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)npl_mod/Raft/LogEntry.lua");
local LogEntry = commonlib.gettable("Raft.LogEntry");
NPL.load("(gl)npl_mod/Raft/LogBuffer.lua");
local LogBuffer = commonlib.gettable("Raft.LogBuffer");

local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua");

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

local openFile;

function SequentialLogStore:new(logContainer) 
    local o = {
        logContainer = logContainer,
        logger = LoggerFactory.getLogger("SequentialLogStore"),
        zeroEntry = LogEntry:new(),
        bufferSize = BUFFER_SIZE,
    };
    setmetatable(o, self);

    o.indexFileName = o.logContainer..LOG_INDEX_FILE
    o.dataFileName = o.logContainer..LOG_STORE_FILE
    o.startIndexFileName = o.logContainer..LOG_START_INDEX_FILE

    o.backupIndexFileName = o.logContainer..LOG_INDEX_FILE_BAK
    o.backupDataFileName = o.logContainer..LOG_STORE_FILE_BAK
    o.backupStartIndexFileName = o.logContainer..LOG_START_INDEX_FILE_BAK

    openFile(o, "r");
    local startIndexFileSize = o.startIndexFile:GetFileSize()
    local indexFileSize = o.indexFile:GetFileSize()

    o.logger.trace("SequentialLogStore:new>startIndexFileSize:%d, indexFileSize:%d", startIndexFileSize, indexFileSize)

    if(startIndexFileSize == 0) then
        openFile(o, "rw");
        o.startIndex = 1;
        o.startIndexFile:WriteDouble(o.startIndex);
    else
        o.startIndex = o.startIndexFile:ReadDouble();
    end

    o.entriesInStore = indexFileSize / DoubleBytes;

    o.buffer = LogBuffer:new((o.entriesInStore > o.bufferSize and (o.entriesInStore + o.startIndex - o.bufferSize)) or o.startIndex, o.bufferSize);

    o:fillBuffer();
    o.logger.debug("log store started with entriesInStore=%d, startIndex=%d", o.entriesInStore, o.startIndex);


    openFile(o, "rw");
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
    self.indexFile:SetFilePointer(0, 2)
    self.dataFile:SetFilePointer(0, 2)
    self.indexFile:WriteDouble(self.dataFile:getpos());
    -- self.logger.trace("datafile pos:%d, indexfile pos:%d", self.dataFile:getpos(), self.indexFile:getpos())
    -- self.dataFile:seek(dataFileLength);
    self.dataFile:WriteDouble(logEntry.term);
    self.dataFile:WriteBytes(1, {logEntry.valueType});
    -- self.dataFile:WriteBytes(#logEntry.value, {logEntry.value:byte(1, -1)});
    self.dataFile:write(logEntry.value, #logEntry.value);

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

    openFile(self, "r")

    local index = logIndex - self.startIndex
    -- find the positions for index and data files
    local dataPosition = self.dataFile:GetFileSize();
    local indexPosition = (index - 1) * DoubleBytes;
    if(indexPosition < self.indexFile:GetFileSize()) then
        self.indexFile:seek(indexPosition);
        dataPosition = self.indexFile:ReadDouble();
    end

    local dataFileSize = self.dataFile:GetFileSize();
    local indexFileSize = self.indexFile:GetFileSize();

    openFile(self, "rw")
    -- write the data at the specified position
    self.indexFile:seek(indexPosition);
    self.dataFile:seek(dataPosition);
    self.indexFile:WriteDouble(dataPosition);
    self.dataFile:WriteDouble(logEntry.term);
    self.dataFile:WriteBytes(1, {logEntry.valueType});
    -- self.dataFile:WriteBytes(#logEntry.value, {logEntry.value:byte(1, -1)});
    self.dataFile:write(logEntry.value, #logEntry.value);

    -- trim the files if necessary
    if(indexFileSize > self.indexFile:getpos()) then
        self.indexFile:SetEndOfFile();
    end

    if(dataFileSize > self.dataFile:getpos()) then
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
    
    -- Assumption: buffer.lastIndex() == this.entriesInStore + this.startIndex
    -- (Yes, for sure, we need to enforce this assumption to be true)
    if(startIndex < bufferFirstIndex) then
        -- in this case, we need to read from store file
        local fileEntries = {}
        local endi = bufferFirstIndex - self.startIndex;

        openFile(self, "r")

        self.indexFile:seek(start * DoubleBytes)
        self.logger.trace("getLogEntries: start bytes:%d, indexfile pos:%d",  start * DoubleBytes, self.indexFile:getpos())
        local dataStart = self.indexFile:ReadDouble();
        for i = 1, (endi - start) do
            local dataEnd = self.indexFile:ReadDouble();
            local dataSize = dataEnd - dataStart;
            self.dataFile:seek(dataStart);
            -- self.logger.trace("getLogEntries: dataStart:%d, dataEnd:%d, indexfile pos:%d, datafile pos:%d", dataStart, dataEnd, self.indexFile:getpos(), self.dataFile:getpos());
            -- here we should use i to index
            fileEntries[i] = self:readEntry(dataSize);
            dataStart = dataEnd;
        end
        for i=1,#entries do
            fileEntries[#fileEntries+1] = entries[i];
        end
        entries = fileEntries


        openFile(self, "rw")
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

    openFile(self, "r")
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
    self.logger.trace("SequentialLogStore:packLog>logIndex:%d, itemsToPack:%d, self.startIndex:%d, entriesInStore:%d",
                       logIndex, itemsToPack, self.startIndex, self.entriesInStore);
    if logIndex < self.startIndex then
        return;
    end

    local index = logIndex - self.startIndex + 1;
    if(index > self.entriesInStore) then
        return {};
    end

    openFile(self, "r")

    local endIndex = math.min(index + itemsToPack, self.entriesInStore + 1);
    local readToEnd = (endIndex == self.entriesInStore + 1);
    local indexPosition = (index - 1) * DoubleBytes;
    self.indexFile:seek(indexPosition);

    local startOfLog = self.indexFile:ReadDouble();
    local endOfLog = self.dataFile:GetFileSize();
    local indexBytes = DoubleBytes * (endIndex - index)
    if(not readToEnd) then
        -- self.indexFile:seekRelative(indexBytes)
        self.indexFile:seek(indexPosition + indexBytes)
        endOfLog = self.indexFile:ReadDouble();
    end

    self.dataFile:seek(startOfLog);

    -- "<memory>" is a special name for memory file, both read/write is possible. 
    local file = ParaIO.open("<memory>", "w");
    local bytes;
    if(file:IsValid()) then
        local dataBytes = endOfLog - startOfLog
        self.logger.trace("SequentialLogStore:packLog>indexBytes:%d, dataBytes:%d", indexBytes, dataBytes)
        file:WriteDouble(indexBytes)
        file:WriteDouble(dataBytes)
       
        -- index data
        local indexBuffer = self.indexFile:GetText(indexPosition, indexBytes)
        assert(#indexBuffer == indexBytes, format("indexBuffer:%d len ~= indexBytes:%d len", #indexBuffer, indexBytes));
        -- writeBytes(file, indexBuffer)
        -- file:WriteBytes(indexBytes, {indexBuffer:byte(1, -1)})
        file:write(indexBuffer, indexBytes)

        -- data
        local dataBuffer = self.dataFile:GetText(startOfLog, dataBytes);
        assert(#dataBuffer == dataBytes, format("dataBuffer:%d len ~= dataBytes:%d len", #dataBuffer, dataBytes));
        -- writeBytes(file, dataBuffer)
        -- file:WriteBytes(dataBytes, {dataBuffer:byte(1, -1)})
        file:write(dataBuffer, dataBytes)


        bytes = file:GetText(0, -1)
        file:close()
	 
        -- Compress
        local data = {content=bytes, method="gzip"};
        if(NPL.Compress(data)) then
            bytes = data.result;
        end
    end
    openFile(self, "rw")
    
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
   self.logger.trace("SequentialLogStore:applyLogPack>logIndex:%d, self.startIndex:%d, entriesInStore:%d",
                     logIndex, self.startIndex, self.entriesInStore);
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
    if(file:IsValid()) then
        file:write(bytes, #bytes)
        file:seek(0)

        local indexBytes = file:ReadDouble()
        local dataBytes = file:ReadDouble()

        local indexBuffer = {}
        local logBuffer = {}
       
        -- index data
        file:ReadBytes(indexBytes, indexBuffer)
        file:ReadBytes(dataBytes, logBuffer)
        assert(#indexBuffer == indexBytes, format("indexBuffer:%d len ~= indexBytes:%d len", #indexBuffer, indexBytes));        
        assert(#logBuffer == dataBytes, format("logBuffer:%d len ~= dataBytes:%d len", #logBuffer, dataBytes));
        self.logger.trace("SequentialLogStore:applyLogPack>indexBytes:%d, dataBytes:%d", indexBytes, dataBytes)

        local indexFilePosition, dataFilePosition;
        openFile(self, "r")
        if(index == self.entriesInStore + 1) then
            indexFilePosition = self.indexFile:GetFileSize();
            dataFilePosition = self.dataFile:GetFileSize();
        else
            indexFilePosition = (index - 1) * DoubleBytes;
            self.indexFile:seek(indexFilePosition);
            dataFilePosition = self.indexFile:ReadDouble();
        end
        openFile(self, "rw")

        self.logger.trace("SequentialLogStore:applyLogPack>indexFilePosition:%d, dataFilePosition:%d", indexFilePosition, dataFilePosition);
        self.indexFile:seek(indexFilePosition);
        self.indexFile:WriteBytes(indexBytes, indexBuffer);
        self.indexFile:SetFilePointer(indexFilePosition+indexBytes, 0);
        self.indexFile:SetEndOfFile();
        
        self.dataFile:seek(dataFilePosition);
        self.dataFile:WriteBytes(dataBytes, logBuffer);
        self.dataFile:SetFilePointer(dataFilePosition+dataBytes, 0);
        self.dataFile:SetEndOfFile();
        self.entriesInStore = index - 1 + indexBytes / DoubleBytes;

        -- openFile(self, "r")
        -- assert(indexFilePosition+indexBytes == self.indexFile:GetFileSize(), format("index pos:%d ~= filesize:%d",indexFilePosition+indexBytes,self.indexFile:GetFileSize()))
        -- assert(dataFilePosition+dataBytes == self.dataFile:GetFileSize(), format("data pos:%d ~= filesize:%d",dataFilePosition+dataBytes,self.dataFile:GetFileSize()))
        -- openFile(self, "rw")

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
        openFile(self, "r")
        local dataPosition = -1;
        local indexPosition = DoubleBytes * (lastIndex + 1);
        self.indexFile:seek(indexPosition);
        dataPosition = self.indexFile:ReadDouble()
        local indexFileNewLength = self.indexFile:GetFileSize() - indexPosition;
        local dataFileNewLength = self.dataFile:GetFileSize() - dataPosition;

        openFile(self, "rw")

        -- copy the log data
        -- data file
        local backupFile = ParaIO.open(self.backupDataFileName, "r");
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
        backupFile = ParaIO.open(self.backupIndexFileName, "r");
        assert(backupFile:IsValid(), "backupFile not Valid")

        
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
        self.buffer:reset(self.entriesInStore > self.bufferSize and self.entriesInStore + self.startIndex - self.bufferSize or self.startIndex);
        self:fillBuffer();
        return true;
    end

    self.logger.error("fail to compact the logs due to error");
    self:restore();
    return false;
end

function SequentialLogStore:fillBuffer()
    openFile(self, "r")
    local startIndex = self.buffer:firstIndex();
    local indexFileSize = self.indexFile:GetFileSize();
    if(indexFileSize > 0) then
        local indexPosition = (startIndex - self.startIndex) * DoubleBytes;
        self.indexFile:seek(indexPosition);
        local dataStart = self.indexFile:ReadDouble();
        self.dataFile:seek(dataStart);
        while(self.indexFile:getpos() < indexFileSize) do
            local dataEnd = self.indexFile:ReadDouble();
            self.logger.trace("SequentialLogStore:fillBuffer>dataStart:%d, dataEnd:%d", dataStart, dataEnd)
            local entry = self:readEntry(dataEnd - dataStart);
            -- util.table_print(entry)
            self.buffer:append(entry);
            dataStart = dataEnd;
        end
        self.logger.trace("SequentialLogStore:fillBuffer>dataStart:%d, dataEnd:%d", dataStart, self.dataFile:GetFileSize())
        local entry = self:readEntry(self.dataFile:GetFileSize() - dataStart);
        self.buffer:append(entry);
    end
    -- self.logger.trace("SequentialLogStore:fillBuffer>buffer firstIndex:%d, entries:%d", self.buffer:firstIndex(), self.buffer:bufferSize())
    openFile(self, "rw")
end


function SequentialLogStore:restore()
    self:closeFiles();

    if not (ParaIO.CopyFile(self.backupIndexFileName, self.indexFileName, true) and
        ParaIO.CopyFile(self.backupDataFileName, self.dataFileName, true) and
        ParaIO.CopyFile(self.backupStartIndexFileName, self.startIndexFileName, true)) then
        -- this is fatal...
        self.logger.fatal("cannot restore from failure, please manually restore the log files");
    end
    openFile(self, "rw")
end

function SequentialLogStore:backup()
    self:close()
    --decide not to use ParaIO.BackupFile
    ParaIO.DeleteFile(self.backupDataFileName)
    ParaIO.DeleteFile(self.backupIndexFileName)
    ParaIO.DeleteFile(self.backupStartIndexFileName)

    if not (ParaIO.CopyFile(self.indexFileName, self.backupIndexFileName, true) and
        ParaIO.CopyFile(self.dataFileName, self.backupDataFileName, true) and
        ParaIO.CopyFile(self.startIndexFileName, self.backupStartIndexFileName, true)) then
        self.logger.error("failed to create a backup folder")
    end

    openFile(self, "rw")
end


function SequentialLogStore:readEntry(size)
    local term = self.dataFile:ReadDouble();
    local valueTypeByte = {}
    self.dataFile:ReadBytes(1, valueTypeByte);
    local valueType = valueTypeByte[1]
    local valueBytes = {}
    -- print(format("SequentialLogStore:readEntry>%d", size-DoubleBytes-1))
    assert(size-DoubleBytes-1 > 0, "size error")
    self.dataFile:ReadBytes(size-DoubleBytes-1, valueBytes);
    -- util.table_print(valueBytes)
    local value = string.char(unpack(valueBytes))
    -- local value = self.dataFile:ReadBytes(size-DoubleBytes-1, nil);
    return LogEntry:new(term, value, valueType);
end

function SequentialLogStore:closeFiles()
    self.indexFile:close()
    self.dataFile:close()
    self.startIndexFile:close()
end
function SequentialLogStore:close()
    self:closeFiles()
    self.prevMode = nil;
end

function openFile(logStore, mode)
    if mode == logStore.prevMode then
        return;
    else
        logStore.prevMode = mode
    end
    if logStore.indexFile and logStore.dataFile and logStore.startIndexFile then
        logStore:closeFiles()
    end
    -- index file
    logStore.indexFile = ParaIO.open(logStore.indexFileName, mode);
    -- assert(logStore.indexFile:IsValid(), "indexFile not Valid")

    -- data file
    logStore.dataFile = ParaIO.open(logStore.dataFileName, mode);
    -- assert(logStore.dataFile:IsValid(), "dataFile not Valid")

    -- startIndex file
    logStore.startIndexFile = ParaIO.open(logStore.startIndexFileName, mode);
    -- assert(logStore.startIndexFile:IsValid(), "startIndexFile not Valid")

    local valid = logStore.indexFile:IsValid() and logStore.dataFile:IsValid() and logStore.startIndexFile:IsValid();
    if not valid then
        mode = "rw"
        return openFile(logStore, mode)
    end
end

function writeBytes(file, indexBuffer)
    if #indexBuffer > 1024 then
        local start_pos = 1
        local end_pos = 1024
        while start_pos < #indexBuffer do
            file:WriteBytes(end_pos - start_pos + 1, {indexBuffer:byte(start_pos, end_pos)})
            start_pos = end_pos + 1;
            end_pos = end_pos + 1024;
        end
    else
        -- file:WriteBytes(#indexBuffer, {indexBuffer:byte(1, -1)})
        file:write(indexBuffer, indexBuffer)
    end
end