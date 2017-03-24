--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/RaftClient.lua");
local RaftClient = commonlib.gettable("Raft.RaftClient");
------------------------------------------------------------
]]--

NPL.load("(gl)script/Raft/LogEntry.lua");
local LogEntry = commonlib.gettable("Raft.LogEntry");
local RaftMessageType = NPL.load("(gl)script/Raft/RaftMessageType.lua");

local RaftClient = commonlib.gettable("Raft.RaftClient");

function RaftClient:new(id, configuration, loggerFactory) 
    local o = {
        id = id,
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



function RaftClient:appendEntries(values)
    if values and #values == 0 then
        return
    end

    local logEntries = {}

    for i=1,#values do
        logEntries[#logEntries + 1] = LogEntry:new(0, values[i])
    end

    request = {
        messageType = RaftMessageType.ClientRequest,
        logEntries = logEntries,
    }

    self:tryCurrentLeader(request, 1, 0)

end


function RaftClient:tryCurrentLeader(request, rpcBackoff, retry)
    self.logger.debug("trying request to %d as current leader from %d", self.leaderId, self.id);

    local o = self
    if (MPRequestRPC("server"..self.id..":", "server"..self.leaderId..":", request, function(err, msg)
                       o.logger.debug("response from remote server, leader: %d, accepted: %s",
                                       response.destination, response.accepted and "true" or "false");
                       if(not response.accepted) then
                           -- set the leader return from the server
                           if(o.leaderId == response.destination and not o.randomLeader) then
                               -- skip here                           
                           else
                               o.randomLeader = false;
                               o.leaderId = response.destination;
                               o:tryCurrentLeader(request, rpcBackoff, retry);
                           end
                       end

                       if callbackFunc then
                           callbackFunc(msg)
                       end
                   end) ~= 0) then
        self.logger.info("rpc error, failed to send request to remote server. tried %d", retry);
        if(retry > #self.configuration.servers) then
            return;
        end

        -- try a random server as leader
        self.leaderId = self.configuration.servers[math.random(#self.configuration.servers)].id;
        self.randomLeader = true;

        if(rpcBackoff > 0) then
            local o = self;
            local timer = commonlib.Timer:new({callbackFunc = function(timer)
                                               o:tryCurrentLeader(request, rpcBackoff + 50, retry + 1);
                                           end})
            timer:Change(rpcBackoff, nil);
        else
            self:tryCurrentLeader(request, rpcBackoff + 50, retry + 1);
        end
    end


end