--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/ServerState.lua");
local ServerState = commonlib.gettable("Raft.ServerState");
------------------------------------------------------------
]]--


local ServerState = commonlib.gettable("Raft.ServerState");

function ServerState:new(t, c, v) 
    local o = {
        term = t or 0,
        commitIndex = c or -1,
        votedFor = v or 0,
    };
    setmetatable(o, self);
    return o;
end

function ServerState:__index(name)
    return rawget(self, name) or ServerState[name];
end

function ServerState:__tostring()
    -- return format("ServerState(term:%d,commitIndex:%d,votedFor:%d)", self.term, self.commitIndex, self.votedFor);
    return util.table_print(self)
end


function ServerState:increaseTerm()
    self.term = self.term + 1
end


function ServerState:setCommitIndex(commitIndex)
    if (commitIndex > self.commitIndex) then
        self.commitIndex = commitIndex;
    end
end