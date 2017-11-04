--[[
Title:
Author: liuluheng
Date: 2017.04.12
Desc:


------------------------------------------------------------
NPL.load("(gl)npl_mod/TableDB/RaftWALLogEntryValue.lua");
local RaftWALLogEntryValue = commonlib.gettable("TableDB.RaftWALLogEntryValue");
------------------------------------------------------------
]]
--
NPL.load("(gl)script/ide/commonlib.lua");
local RaftWALLogEntryValue = commonlib.gettable("TableDB.RaftWALLogEntryValue");

local memoryFile = ParaIO.open("<memory>", "w");

function RaftWALLogEntryValue:new(rootFolder, collectionName, page_data, pgno, nTruncate, isCommit)
    local o = {
        rootFolder = rootFolder,
        collectionName = collectionName,
        page_data = page_data,
        pgno = pgno,
        nTruncate = nTruncate,
        isCommit = isCommit,
    }
    setmetatable(o, self);
    return o;
end

function RaftWALLogEntryValue:__index(name)
    return rawget(self, name) or RaftWALLogEntryValue[name];
end

function RaftWALLogEntryValue:__tostring()
    return util.table_tostring(self)
end


function RaftWALLogEntryValue:fromBytes(bytes)
    local file = ParaIO.open("<memory>", "rw");
    local o = {}
    if (file:IsValid()) then
        -- local bytes = {bytes:byte(1, -1)}
        -- write not worked! use error ? string:char maybe
        file:write(bytes, #bytes)
        -- file:WriteBytes(#bytes, bytes)
        file:seek(0)
        
        -- rootFolder
        local rootFolderLen = file:ReadInt()
        o.rootFolder = file:ReadString(rootFolderLen)
        -- collectionName
        local collectionNameLen = file:ReadInt()
        o.collectionName = file:ReadString(collectionNameLen)
        -- page_data
        local page_data_len = file:ReadInt()
        o.page_data = {}
        file:ReadBytes(page_data_len, o.page_data)
        o.page_data = string.char(unpack(o.page_data))
        -- pgno
        o.pgno = file:ReadInt()
        -- nTruncate
        o.nTruncate = file:ReadInt()
        -- isCommit
        o.isCommit = file:ReadInt()
        
        file:close()
        setmetatable(o, self);
        return o;
    end
end


function RaftWALLogEntryValue:toBytes()
    local file = ParaIO.open("<memory>", "rw");
    local bytes;
    if (file:IsValid()) then
        
        -- rootFolder
        file:WriteInt(#self.rootFolder)
        file:WriteString(self.rootFolder)
        -- collectionName
        file:WriteInt(#self.collectionName)
        file:WriteString(self.collectionName)
        -- page_data
        local page_data_bytes = {self.page_data:byte(1, -1)}
        file:WriteInt(#page_data_bytes)
        file:WriteBytes(#page_data_bytes, page_data_bytes)
        -- pgno
        file:WriteInt(self.pgno)
        -- nTruncate
        file:WriteInt(self.nTruncate)
        -- isCommit
        file:WriteInt(self.isCommit)
        
        bytes = file:GetText(0, -1)
        file:close()
    end
    return bytes;
end
