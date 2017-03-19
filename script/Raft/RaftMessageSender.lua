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
    -- return format("RaftMessageSender(term:%d,commitIndex:%d,votedFor:%d)", self.term, self.commitIndex, self.votedFor);
    return util.table_tostring(self)
end



function RaftMessageSender:appendEntries(values)
    if(values == nil or #values == 0) then
        return;
    end

    logEntries = {}
    for i,v in ipairs(values) do
        logEntries[i] = LogEntry:new(0, values[i]);
    end

    request = {
        messageType = RaftMessageType.ClientRequest,
        logEntries = logEntries,
    }

    return self:sendMessageToLeader(request);
end

function RaftMessageSender:sendMessageToLeader(request)
    leaderId = self.server.leader;
    config = this.server.config;
    if(leaderId == -1) then
        return;
    end

    if(leaderId == this.server.id) then
        return self.server:processRequest(request);
    end

    -- CompletableFuture<Boolean> result = new CompletableFuture<Boolean>();
    -- RpcClient rpcClient = this.rpcClients.get(leaderId);
    -- if(rpcClient == null){
    --     ClusterServer leader = config.getServer(leaderId);
    --     if(leader == null){
    --         result.complete(false);
    --         return result;
    --     }

    --     rpcClient = this.server.context.getRpcClientFactory().createRpcClient(leader.getEndpoint());
    --     this.rpcClients.put(leaderId, rpcClient);
    -- }

    -- rpcClient.send(request).whenCompleteAsync((RaftResponseMessage response, Throwable err) -> {
    --     if(err != null){
    --         this.server.logger.info("Received an Rpc error %s while sending a request to server (%d)", err.getMessage(), leaderId);
    --         result.complete(false);
    --     }else{
    --         result.complete(response.isAccepted());
    --     }
    -- }, this.server.context.getScheduledExecutor());

end