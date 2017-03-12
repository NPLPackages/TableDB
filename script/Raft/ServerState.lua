--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft.ServerState.lua");
local ServerState = commonlib.gettable("Raft.ServerState");
------------------------------------------------------------
]]--



local ServerState = commonlib.gettable("Raft.ServerState");

function ServerState:new() 
    local o = {
        term = 0,
        commitIndex = 0,
        votedFor = 0,
    };
    setmetatable(o, self);
    return o;
end

function ServerState:__index(name)
	return rawget(self, name) or ServerState[name];
end

function ServerState:increaseTerm()
    self.term = self.term + 1
end


function ServerState:setCommitIndex(commitIndex)
    if (commitIndex > self.commitIndex) then
        self.commitIndex = commitIndex;
    end
end

function ServerState:tostring()
    return "ServerState=term:"..self.term ..
           ", commitIndex:" .. self.commitIndex ..
           ", votedFor:" ..  self.votedFor
end