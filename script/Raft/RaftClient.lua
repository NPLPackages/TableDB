--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/RaftClient.lua");
local RaftClient = commonlib.gettable("Raft.RaftClient");
------------------------------------------------------------
]]--

NPL.load("(gl)script/Raft/LogEntry.lua");
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
local LogEntry = commonlib.gettable("Raft.LogEntry");
local RaftMessageType = NPL.load("(gl)script/Raft/RaftMessageType.lua");

local RaftClient = commonlib.gettable("Raft.RaftClient");

function RaftClient:new(localAddress, RequestRPC, configuration, loggerFactory) 
    local o = {
        localAddress = localAddress,
        RequestRPC = RequestRPC,
        configuration = configuration,
        leaderId = configuration.servers[math.random(#configuration.servers)].id,
        randomLeader = true,
        logger = loggerFactory.getLogger("RaftClient"),
    }
    setmetatable(o, self);
    return o;
end

function RaftClient:__index(name)
    return rawget(self, name) or RaftClient[name];
end

function RaftClient:__tostring()
    return util.table_tostring(self)
end



function RaftClient:appendEntries(values, callbackFunc)
    if values and #values == 0 then
        return
    end

    local logEntries = {}

    for i,v in ipairs(values) do
        logEntries[#logEntries + 1] = LogEntry:new(0, v)
    end

    request = {
        messageType = RaftMessageType.ClientRequest,
        logEntries = logEntries,
    }

    self:tryCurrentLeader(request, callbackFunc, 500, 0)
end


function RaftClient:addServer(server, callbackFunc)
    if(server == nil) then
        self.logger.error("server cannot be null");
        return
    end

    local logEntries = {LogEntry:new(0, server:toBytes(), LogValueType.ClusterServer)};
    local request = {
        messageType = RaftMessageType.AddServerRequest,
        logEntries = logEntries,
    }

    self:tryCurrentLeader(request, callbackFunc, 500, 0);
end


function RaftClient:removeServer(serverId, callbackFunc)
    if(serverId < 0) then
        self.logger.error("serverId must be equal or greater than zero");
        return
    end

    -- assume {serverId} is string
    local logEntries = {LogEntry:new(0, {serverId}, LogValueType.ClusterServer)};
    local request = {
        messageType = RaftMessageType.RemoveServerRequest,
        logEntries = logEntries,
    }

    self:tryCurrentLeader(request, callbackFunc, 500, 0);
end


function RaftClient:tryCurrentLeader(request, callbackFunc, rpcBackoff, retry)
    self.logger.debug("trying request to %d as current leader from %s", self.leaderId, self.localAddress.id);

    local o = self

    local backoff_retry_func = function (...)
        if(rpcBackoff > 0) then
            local backoff_timer = commonlib.Timer:new({callbackFunc = function(timer)
                                               o:tryCurrentLeader(request, callbackFunc, rpcBackoff + 500, retry + 1);
                                           end})
            backoff_timer:Change(rpcBackoff, nil);
        else
            o:tryCurrentLeader(request, callbackFunc, rpcBackoff + 500, retry + 1);
        end
    end
    local activate_result = self.RequestRPC(self.localAddress, "server"..self.leaderId..":", request, function(err, response)
                       if not err then
                           o.logger.info("response from remote server, leader: %d, accepted: %s",
                                           response.destination, response.accepted and "true" or "false");
                           if(not response.accepted) then
                               -- set the leader return from the server
                               if(o.leaderId == response.destination and not o.randomLeader) then
                                   -- skip here
                               else
                                   o.randomLeader = false;
                                   o.leaderId = response.destination;
                               end
                               return o:tryCurrentLeader(request, callbackFunc, rpcBackoff, retry);
                           end

                           if callbackFunc then
                               callbackFunc(err, response)
                           end
                       elseif err == "timeout" then
                           -- we handle connected here
                        --    o:tryCurrentLeader(request, rpcBackoff, retry);
                           backoff_retry_func()
                       else
                           self.logger.info("rpc error, failed(%d) to send request to remote server, err:%s. tried %d, no more try here", activate_result, err, retry);
                       end
                   end, 1);
    if (activate_result ~= 0) then
        self.logger.info("rpc error, failed(%d) to send request to remote server. tried %d", activate_result, retry);
        if(retry > 3 * #self.configuration.servers) then
            self.logger.info("FAILED. reach to the max retry. tried %d", retry);
            if callbackFunc then
                callbackFunc("FAILED")
            end
            return;
        end

        -- try a random server as leader
        self.leaderId = self.configuration.servers[math.random(#self.configuration.servers)].id;
        self.randomLeader = true;

        backoff_retry_func()
    end



end