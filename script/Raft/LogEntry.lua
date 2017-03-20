--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/LogEntry.lua");
local LogEntry = commonlib.gettable("Raft.LogEntry");
------------------------------------------------------------
]]--

local LogValueType = NPL.load("(gl)script/Raft/LogValueType.lua");
local LogEntry = commonlib.gettable("Raft.LogEntry");

function LogEntry:new(term, value, valueType) 
    local o = {
        value = value or nil,
        term = term or 0,
        valueType = valueType or LogValueType.Application,
    };
    setmetatable(o, self);
    return o;
end

function LogEntry:__index(name)
    return rawget(self, name) or LogEntry[name];
end

function LogEntry:__tostring()
    return util.table_tostring(self)
end