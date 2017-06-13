--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/SnapshotSyncRequest.lua");
local SnapshotSyncRequest = commonlib.gettable("Raft.SnapshotSyncRequest");
------------------------------------------------------------
]]--

NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)npl_mod/Raft/ClusterConfiguration.lua");
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration");
NPL.load("(gl)npl_mod/Raft/Snapshot.lua");
local Snapshot = commonlib.gettable("Raft.Snapshot");

local SnapshotSyncRequest = commonlib.gettable("Raft.SnapshotSyncRequest");

function SnapshotSyncRequest:new(snapshot, offset, data, done, currentCollectionName) 
    local o = {
        snapshot = snapshot,
        offset = offset,
        data = data,
        done = done,

        -- extended for TableDB collections
        currentCollectionName = currentCollectionName,
    };
    setmetatable(o, self);
    return o;
end

function SnapshotSyncRequest:__index(name)
    return rawget(self, name) or SnapshotSyncRequest[name];
end

function SnapshotSyncRequest:__tostring()
    return util.table_tostring(self)
end



function SnapshotSyncRequest:toBytes()
    local configData = self.snapshot.lastConfig:toBytes();
    
    -- "<memory>" is a special name for memory file, both read/write is possible. 
	local file = ParaIO.open("<memory>", "w");
    local bytes;
	if(file:IsValid()) then
        file:WriteDouble(self.snapshot.lastLogIndex)
        file:WriteDouble(self.snapshot.lastLogTerm)
        file:WriteInt(#configData)
        -- file:WriteBytes(#configData, {configData:byte(1, -1)})
        file:write(configData, #configData)
        file:WriteDouble(self.offset)
        file:WriteInt(#self.data)
        -- if type(self.data) == "string" then
        file:WriteBytes(#self.data, self.data)
        -- file:write(self.data, #self.data)
        -- end
        file:WriteBytes(1, {(self.done and 1) or 0})

        file:WriteInt(#self.currentCollectionName);
        file:WriteString(self.currentCollectionName);

        bytes = file:GetText(0, -1)

        file:close()
    end
    return bytes;
end


function SnapshotSyncRequest:fromBytes(bytes)
    -- "<memory>" is a special name for memory file, both read/write is possible. 
	local file = ParaIO.open("<memory>", "w");
    local snapshotSyncRequest;
	if(file:IsValid()) then
        -- can not use file:WriteString(bytes);, use WriteBytes
        -- file:WriteBytes(#bytes, {bytes:byte(1, -1)});
        file:write(bytes, #bytes);
        file:seek(0)
        local lastLogIndex = file:ReadDouble()
        local lastLogTerm = file:ReadDouble()
        local configSize = file:ReadInt()
        local configData = {}
        file:ReadBytes(configSize, configData)
        local config = ClusterConfiguration:fromBytes(configData)
        local offset = file:ReadDouble()
        local dataSize = file:ReadInt()
        local data = {}
        if dataSize ~= 0 then
            file:ReadBytes(dataSize, data)
            -- data = string.char(unpack(data))
        end
        local doneByte = {}
        file:ReadBytes(1, doneByte)
        local done = doneByte[1] == 1

        local currentCollectionNameLen = file:ReadInt();
        local currentCollectionName = file:ReadString(currentCollectionNameLen);

        snapshotSyncRequest = SnapshotSyncRequest:new(Snapshot:new(lastLogIndex, lastLogTerm, config), offset, data, done, currentCollectionName);

        file:close()
    end
    return snapshotSyncRequest;
end