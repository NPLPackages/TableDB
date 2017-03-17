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
local logger = commonlib.logging.GetLogger("")

local RaftConsensus = commonlib.gettable("Raft.RaftConsensus");
-- RaftConsensus = {}


function RaftConsensus.run(context)
    if(context == nil) then
        logger.error("context cannot be null");
        return;
    end

    server = raftServer:new(context);
    -- messageSender = server:createMessageSender();
    -- context.getStateMachine().start(messageSender);
    context.getRpcListener().startListening(server);
    return messageSender;
end


function RaftConsensus:__tostring()
    return util.table_print(self)
end
