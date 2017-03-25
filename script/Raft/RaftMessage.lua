--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/RaftMessage.lua");
local RaftMessage = commonlib.gettable("Raft.RaftMessage");
------------------------------------------------------------
]]--


local RaftMessage = commonlib.gettable("Raft.RaftMessage");

function RaftMessage:new(subfields) 
    local o = {
        messageType = 0,
        source = 0,
        destination =  0,
        term = 0,
    };
    if subfields then
        for k,v in pairs(subfields) do
            o[k] = v
        end
    end
    setmetatable(o, self);
    return o;
end

function RaftMessage:__index(name)
    return rawget(self, name) or RaftMessage[name];
end

function RaftMessage:__tostring()
    return util.table_tostring(self)
end
