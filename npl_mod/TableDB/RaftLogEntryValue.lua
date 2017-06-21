--[[
Title: 
Author: liuluheng
Date: 2017.04.12
Desc: 


------------------------------------------------------------
NPL.load("(gl)npl_mod/TableDB/RaftLogEntryValue.lua");
local RaftLogEntryValue = commonlib.gettable("TableDB.RaftLogEntryValue");
------------------------------------------------------------
]]--

NPL.load("(gl)script/ide/commonlib.lua");
local RaftLogEntryValue = commonlib.gettable("TableDB.RaftLogEntryValue");

function RaftLogEntryValue:new(query_type, collection, query, index, serverId, enableSyncMode, callbackThread) 
    local o = {
      query_type = query_type, 
      collection = collection,
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
    local file = ParaIO.open("<memory>", "w");
    if(file:IsValid()) then  
        if type(bytes) == "string" then
            file:write(bytes, #bytes);
        elseif type(bytes) == "table" then
            file:WriteBytes(#bytes, bytes);
        end
        file:seek(0)

        local n = file:ReadInt();
        local str = file:ReadString(n)
        -- print(str)
        local o = commonlib.LoadTableFromString(str)
        if not o then
            str = string.gsub( str, "([%+%-][%a+%+%-]+)", "[\"%1\"]")
            o = commonlib.LoadTableFromString(str)
        end
        setmetatable(o, self);
        return o;
    end
end


function RaftLogEntryValue:toBytes()
   local data = {
       query_type = self.query_type,
       collection = self.collection:ToData(),
       query = self.query,
       cb_index = self.cb_index,
       serverId = self.serverId,
       enableSyncMode = self.enableSyncMode,
       callbackThread = self.callbackThread,
   }
   local str = commonlib.serialize_compact2(data)
   -- print(str)
   -- "<memory>" is a special name for memory file, both read/write is possible.
   local file = ParaIO.open("<memory>", "w");
   local bytes;
   if(file:IsValid()) then
        -- -- query_type
        -- file:WriteInt(#self.query_type)
        -- file:WriteString(self.query_type)

        -- -- collection
        -- file:WriteInt(#self.collection.name)
        -- file:WriteString(self.collection.name)
        -- file:WriteInt(#self.collection.db)
        -- file:WriteString(self.collection.db)

        -- query
        -- query is a dict or a nested dict

        file:WriteInt(#str)
        file:WriteString(str)


        bytes = file:GetText(0, -1)

        file:close()
    end
    return bytes;
end

