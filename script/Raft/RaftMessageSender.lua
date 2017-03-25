--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/RaftMessageSender.lua");
local RaftMessageSender = commonlib.gettable("Raft.RaftMessageSender");
------------------------------------------------------------
]]--
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)script/Raft/Rpc.lua");
local Rpc = commonlib.gettable("Raft.Rpc");
local RaftMessageSender = commonlib.gettable("Raft.RaftMessageSender");

function RaftMessageSender:new(server) 
    local o = {
        server = server,
    };
    setmetatable(o, self);
    return o;
end

function RaftMessageSender:__index(name)
    return rawget(self, name) or RaftMessageSender[name];
end

function RaftMessageSender:__tostring()
    return util.table_tostring(self)
end

function RaftMessageSender:appendEntries(values)
    -- util.table_print(values)
    -- if(values or #values == 0) then
    --     return;
    -- end

    -- local logEntries = {};
    -- for i = 1, #values do
    --     logEntries[#logEntries+1] =  LogEntry:new(0, values[i]);
    -- end

    -- local request = {
    --     messageType = RaftMessageType.ClientRequest,
    --     logEntries = logEntries,
    -- }
    -- print("iiiii")
    
    -- util.table_print(request)

    return self:sendMessageToLeader(values);
end

function RaftMessageSender:sendMessageToLeader(request)
    leaderId = self.server.leader;
    config = self.server.config;
    -- print(format("leaderId %d", leaderId))
    
    if(leaderId == -1) then
        return;
    end


    if(leaderId == self.server.id) then
        return self.server:processRequest(request);
    end


    if (RaftRequestRPC("server"..self.server.id..":", "server"..leaderId..":", request, function(err, msg)
                       o:resumeHeartbeatingSpeed();

                       if callbackFunc then
                           callbackFunc(msg)
                       end
                   end) ~= 0) then
        self:slowDownHeartbeating()
    end

end