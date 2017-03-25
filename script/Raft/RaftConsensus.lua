--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/RaftConsensus.lua");
local RaftConsensus = commonlib.gettable("Raft.RaftConsensus");
------------------------------------------------------------
]]--

NPL.load("(gl)script/Raft/RaftServer.lua");
local raftServer = commonlib.gettable("Raft.RaftServer");
local LoggerFactory = NPL.load("(gl)script/Raft/LoggerFactory.lua");

local logger = LoggerFactory.getLogger("RaftConsensus")

local RaftConsensus = commonlib.gettable("Raft.RaftConsensus");
-- RaftConsensus = {}


function RaftConsensus.run(context)
    if(context == nil) then
        logger.error("context cannot be null");
        return;
    end

    local server = raftServer:new(context);
    local messageSender = server:createMessageSender();
    context.stateMachine:start(messageSender);
    context.rpcListener:startListening(server);
    return messageSender;
end


function RaftConsensus:__tostring()
    return util.table_tostring(self)
end
