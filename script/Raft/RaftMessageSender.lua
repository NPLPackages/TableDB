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

local LoggerFactory = NPL.load("(gl)script/Raft/LoggerFactory.lua");

local logger = LoggerFactory.getLogger("RaftMessageSender")
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)script/Raft/Rpc.lua");
local Rpc = commonlib.gettable("Raft.Rpc");
local RaftMessageType = NPL.load("(gl)script/Raft/RaftMessageType.lua");
local RaftMessageSender = commonlib.gettable("Raft.RaftMessageSender");

function RaftMessageSender:new(server) 
    local o = {
        server = server,
        logger = logger,
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
    return self:sendMessageToLeader(values);
end

function RaftMessageSender:sendMessageToLeader(request)
    leaderId = self.server.leader;
    config = self.server.config;
    
    if(leaderId == -1) then
        self.logger.error("no leader in the cluster now")
        return;
    end


    if(leaderId == self.server.id) then
        return self.server:processRequest(request);
    else
        local response = {
            messageType = RaftMessageType.AppendEntriesResponse,
            destination = leaderId,
            accepted = false,
        }
        return response;
    end

    -- should we forward to the leader ?
    if (RaftRequestRPC("server"..self.server.id..":", "server"..leaderId..":", request, function(err, msg)
                       o:resumeHeartbeatingSpeed();

                       if callbackFunc then
                           callbackFunc(msg)
                       end
                   end) ~= 0) then
        self:slowDownHeartbeating()
    end

end