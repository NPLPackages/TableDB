--[[
Title:
Author: liuluheng
Date: 2017.03.25
Desc:
the WALLogBuffer is startIndex based
------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/WALLogBuffer.lua");
local WALLogBuffer = commonlib.gettable("Raft.WALLogBuffer");
------------------------------------------------------------
]]
--
local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua");
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)npl_mod/Raft/Rutils.lua");
local Rutils = commonlib.gettable("Raft.Rutils");
local WALLogBuffer = commonlib.gettable("Raft.WALLogBuffer");


function WALLogBuffer:new(startIndex, maxSize)
    local o = {
        startIndex = startIndex,
        entriesInBuffer = 0,
        maxSize = maxSize,
        logger = LoggerFactory.getLogger("WALLogBuffer"),
        buffer = {},
    };
    setmetatable(o, self);
    return o;
end

function WALLogBuffer:__index(name)
    return rawget(self, name) or WALLogBuffer[name];
end

function WALLogBuffer:__tostring()
    return util.table_tostring(self)
end



function WALLogBuffer:lastIndex()
    return self.startIndex + self.entriesInBuffer - 1;
end


function WALLogBuffer:firstIndex()
    return self.startIndex;
end


function WALLogBuffer:lastEntry()
    -- if buffer size = 0, will return nil
    return self.buffer[self:lastIndex()]
end

function WALLogBuffer:entryAt(index)
    -- self.logger.trace("entryAt>index:%d, self.startIndex:%d, self.buffer len:%d",
    --                    index, self.startIndex, self:bufferSize())
    return self.buffer[index];
end

function WALLogBuffer:writeAt(index, logEntry)
    if index < self:firstIndex() and index > self:lastIndex() + 1 then
        self.logger.error("badly wrong!! this must be a bug!");
        return;
    end
    self.buffer[index] = logEntry;
    if index == self:lastIndex() + 1 then
        self.entriesInBuffer = self.entriesInBuffer + 1;
    end
end

-- [start, end), returns the startIndex
function WALLogBuffer:fill(start, endi, result)
    if endi < self.startIndex then
        return self.startIndex;
    end
    
    if start < self.startIndex then
        start = self.startIndex
    end
    for i = start, endi - 1 do
        result[#result + 1] = self:entryAt(i)
    end
    
    self.logger.trace("fill>start:%d, end:%d, result len:%d, self.startIndex:%d, self.buffer len:%d",
        start, endi, #result, self.startIndex, self:bufferSize())
    -- self.logger.trace("fill>result:%s", util.table_tostring(result))
    return self.startIndex;
end


-- trimming the buffer [fromIndex, end)
function WALLogBuffer:trim(fromIndex)
    if fromIndex < self:lastIndex() + 1 then
        for i = fromIndex, self:lastIndex() do
            self.buffer[i] = nil
            self.entriesInBuffer = self.entriesInBuffer - 1;
        end
    end
end

function WALLogBuffer:append(entry)
    
    self.buffer[self:lastIndex() + 1] = entry
    self.entriesInBuffer = self.entriesInBuffer + 1;
-- self.logger.trace("append>index:%d->%s, self.startIndex:%d, self.buffer len:%d",
--                    self:lastIndex(), util.table_tostring(entry), self.startIndex, self:bufferSize())
-- make unbound
-- maxSize
-- if self.maxSize < self.entriesInBuffer then
--     self.buffer[self.startIndex] = nil
--     self.startIndex = self.startIndex + 1
--     self.entriesInBuffer = self.entriesInBuffer - 1;
-- end
end

function WALLogBuffer:reset(startIndex)
    if startIndex > self.startIndex then
        for i = self.startIndex, startIndex - 1 do
            self.buffer[i] = nil
            self.entriesInBuffer = self.entriesInBuffer - 1;
        end
    end
    self.startIndex = startIndex
end

function WALLogBuffer:bufferSize()
    return self.entriesInBuffer
end