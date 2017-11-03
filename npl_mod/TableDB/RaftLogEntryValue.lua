--[[
Title:
Author: liuluheng
Date: 2017.04.12
Desc:


------------------------------------------------------------
NPL.load("(gl)npl_mod/TableDB/RaftLogEntryValue.lua");
local RaftLogEntryValue = commonlib.gettable("TableDB.RaftLogEntryValue");
------------------------------------------------------------
]]
--
NPL.load("(gl)script/ide/commonlib.lua");
local RaftLogEntryValue = commonlib.gettable("TableDB.RaftLogEntryValue");

local memoryFile = ParaIO.open("<memory>", "w");

function RaftLogEntryValue:new(query_type, collection, query, index, serverId, enableSyncMode, callbackThread)
    local o = {
        query_type = query_type,
        collection = collection:ToData(),
        query = query,
        cb_index = index,
        serverId = serverId,
        enableSyncMode = enableSyncMode,
        callbackThread = callbackThread,
    };
    setmetatable(o, self);
    return o;
end

function RaftLogEntryValue:__index(name)
    return rawget(self, name) or RaftLogEntryValue[name];
end

function RaftLogEntryValue:__tostring()
    return util.table_tostring(self)
end


function RaftLogEntryValue:fromBytes(bytes)
    if (memoryFile:IsValid()) then
        memoryFile:seek(0)
        if type(bytes) == "string" then
            memoryFile:write(bytes, #bytes);
        elseif type(bytes) == "table" then
            memoryFile:WriteBytes(#bytes, bytes);
        end
        memoryFile:seek(0)
        
        local n = memoryFile:ReadInt();
        local str = memoryFile:ReadString(n)
        -- print(str)
        local o = commonlib.LoadTableFromString(str)
        if not o then
            str = string.gsub(str, "([%+%-][%a+%+%-]+)", "[\"%1\"]")
            o = commonlib.LoadTableFromString(str)
        end
        setmetatable(o, self);
        return o;
    end
end


function RaftLogEntryValue:toBytes()
    local str = commonlib.serialize_compact2(self)
    
    local bytes;
    if (memoryFile:IsValid()) then
        -- -- query_type
        -- memoryFile:WriteInt(#self.query_type)
        -- memoryFile:WriteString(self.query_type)
        -- -- collection
        -- memoryFile:WriteInt(#self.collection.name)
        -- memoryFile:WriteString(self.collection.name)
        -- memoryFile:WriteInt(#self.collection.db)
        -- memoryFile:WriteString(self.collection.db)
        -- query
        -- query is a dict or a nested dict
        memoryFile:seek(0)
        memoryFile:WriteInt(#str)
        memoryFile:WriteString(str)
        
        bytes = memoryFile:GetText(0, -1)
    end
    return bytes;
end
