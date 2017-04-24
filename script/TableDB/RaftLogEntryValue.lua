--[[
Title: 
Author: liuluheng
Date: 2017.04.12
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/TableDB/RaftLogEntryValue.lua");
local RaftLogEntryValue = commonlib.gettable("TableDB.RaftLogEntryValue");
------------------------------------------------------------
]]--

NPL.load("(gl)script/ide/commonlib.lua");
local RaftLogEntryValue = commonlib.gettable("TableDB.RaftLogEntryValue");

function RaftLogEntryValue:new(query_type, collection, query) 
    local o = {
      query_type = query_type, 
      collection = collection,
      query = query,
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
        local o = commonlib.LoadTableFromString(str)
        setmetatable(o, self);
        return o;
    end
end


function RaftLogEntryValue:toBytes()
   local str = commonlib.serialize_compact2(self)
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

