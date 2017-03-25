--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 
the LogBuffer is startIndex based
------------------------------------------------------------
NPL.load("(gl)script/Raft/LogBuffer.lua");
local LogBuffer = commonlib.gettable("Raft.LogBuffer");
------------------------------------------------------------
]]--

local LoggerFactory = NPL.load("(gl)script/Raft/LoggerFactory.lua");
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
local LogBuffer = commonlib.gettable("Raft.LogBuffer");


function LogBuffer:new(startIndex, maxSize) 
    local o = {
        startIndex = startIndex,
        maxSize = maxSize,
        logger = LoggerFactory.getLogger("LogBuffer"),
        buffer = {},
    };
    setmetatable(o, self);
    return o;
end

function LogBuffer:__index(name)
    return rawget(self, name) or LogBuffer[name];
end

function LogBuffer:__tostring()
    return util.table_tostring(self)
end



function LogBuffer:lastIndex()
    return self.startIndex + #self.buffer
end


function LogBuffer:startIndex()
    return self.startIndex;
end


function LogBuffer:lastEntry()
    -- if buffer size = 0, will return nil
    return self.buffer[self.startIndex + #self.buffer]
end

function LogBuffer:entryAt(index)
    return self.buffer[index];
end

-- [start, end), returns the startIndex
function LogBuffer:fill(start, endi, result)
    if endi < self.startIndex then
        return self.startIndex;
    end

    for i=start, endi do
        result[#result + 1] = self.buffer[i]
    end

    self.logger.trace("LogBuffer:fill>start:%d, end:%d, result len:%d, self.buffer len:%d",
                       start, endi, #result, #self.buffer)
    -- self.logger.trace("LogBuffer:fill>result:%s", util.table_tostring(result))

    return self.startIndex;
end


-- trimming the buffer [fromIndex, end)
function LogBuffer:trim(fromIndex)
    local index = fromIndex - self.startIndex;
    
    if index < #self.buffer + 1 then
        for i=index, #self.buffer do
            self.buffer[self.startIndex+i] = nil
        end
    end
end

function LogBuffer:append(entry)
    self.buffer[#self.buffer+1] = entry

    -- maxSize
    if self.maxSize < #self.buffer + 1 then
        self.buffer[self.startIndex] = nil
        self.startIndex = self.startIndex + 1
    end
end

function LogBuffer:reset(startIndex)
    self.buffer = {}
    self.startIndex = startIndex
end

