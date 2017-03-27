--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/SnapshotSyncRequest.lua");
local SnapshotSyncRequest = commonlib.gettable("Raft.SnapshotSyncRequest");
------------------------------------------------------------
]]--


local SnapshotSyncRequest = commonlib.gettable("Raft.SnapshotSyncRequest");

function SnapshotSyncRequest:new(snapshot, offset, data, done) 
    local o = {
        snapshot = snapshot,
        offset = offset,
        data = data,
        done = done,
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
        file:WriteUInt(self.lastLogIndex)
        file:WriteUInt(self.lastLogTerm)
        file:WriteInt(#configData)
        file:WriteBytes(#configData, {configData:byte(1, -1)})
        file:WriteUInt(self.offset)
        file:WriteInt(#self.data)
        file:WriteBytes(#self.data, {self.data:byte(1, -1)})
        file:WriteBytes(1, {(self.done and 1) or 0})

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
        file:WriteBytes(#bytes, {bytes:byte(1, -1)});
        file:seek(0)
        local lastLogIndex = file:ReadUInt()
        local lastLogTerm = file:ReadUInt()
        local configSize = file:ReadInt(#configData)
        local configData = {}
        file:ReadBytes(configSize, configData)
        local config = ClusterConfiguration:fromBytes(configData)
        local offset = file:ReadUInt()
        local dataSize = file:ReadInt()
        local data = {}
        file:ReadBytes(dataSize, data)
        local doneByte = {}
        file:ReadBytes(1, done)
        local done = doneByte[1] == 1

        snapshotSyncRequest = SnapshotSyncRequest:new(Snapshot:new(lastLogIndex, lastLogTerm, config), offset, data, done);

        file:close()
    end
    return snapshotSyncRequest;
end