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
NPL.load("(gl)npl_mod/TableDB/VectorPool.lua");
local VectorPool = commonlib.gettable("TableDB.VectorPool");

local memoryFile;
-- set this to true, if one wants to use binary format (currently slower than text format)
local is_binary_format = false;
if(is_binary_format) then
    memoryFile = ParaIO.open("<memory>", "w");
end

function RaftLogEntryValue:new(query_type, collection, query, index, serverId, enableSyncMode, callbackThread)
    local o = {
        query_type = query_type,
        collection = collection:ToData(),
        query = query,
        cb_index = index,
        serverId = serverId,
        callbackThread = callbackThread,
        enableSyncMode = enableSyncMode,
    };
    setmetatable(o, self);
    return o;
end

function RaftLogEntryValue:new_from_pool(query_type, collection, query, index, serverId, enableSyncMode, callbackThread)
	return VectorPool.GetSingleton():GetVector(query_type, collection, query, index, serverId, enableSyncMode, callbackThread);	
end


function RaftLogEntryValue:set(query_type, collection, query, index, serverId, enableSyncMode, callbackThread)
    self.query_type = query_type;
    self.collection = collection:ToData();
    self.query = query;
    self.cb_index = index;
    self.serverId = serverId;
    self.callbackThread = callbackThread;
    self.enableSyncMode = enableSyncMode;
end

function RaftLogEntryValue:__index(name)
    return rawget(self, name) or RaftLogEntryValue[name];
end

function RaftLogEntryValue:__tostring()
    return util.table_tostring(self)
end

-- this is actual msg table we used for exchanging data, slightly more compact
local msg_ = {};
function RaftLogEntryValue:fromBytes(bytes)
    if(is_binary_format) then
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
    else
        local t = NPL.LoadTableFromString(bytes)
        if(t) then
            -- decode from msg_
            local o = {
                query_type = t[1],
                collection = t[2],
                query = t[3],
                cb_index = t[4],
                serverId = t[5],
                callbackThread =t[6],
                enableSyncMode = t[7],
            }
            setmetatable(o, self);
            return o;
        else
            LOG.std(nil, "warn", "RaftLogEntryValue:fromBytes", "can not parse bytes")
        end
    end
end


function RaftLogEntryValue:toBytes()
    if(is_binary_format) then
        local str = commonlib.serialize_compact(self)
        
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
            local nSize = #str;
            memoryFile:WriteInt(nSize)
            memoryFile:write(str, nSize);
            bytes = memoryFile:GetText(0, memoryFile:getpos());
        end
        return bytes;
    else
        -- encode into msg_
        msg_[1] = self.query_type;
        msg_[2] = self.collection;
        msg_[3] = self.query;
        msg_[4] = self.cb_index;
        msg_[5] = self.serverId;
        msg_[6] = self.callbackThread;
        msg_[7] = self.enableSyncMode;
        return commonlib.serialize_compact(msg_);
    end
end
