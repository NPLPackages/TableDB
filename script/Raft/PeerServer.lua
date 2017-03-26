--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 

Peer server in the same cluster for local server
this represents a peer for local server, it could be a leader, however, if local server is not a leader, though it has a list of peer servers, they are not used

------------------------------------------------------------
NPL.load("(gl)script/Raft/PeerServer.lua");
local PeerServer = commonlib.gettable("Raft.PeerServer");
------------------------------------------------------------
]]--

NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")

local RaftMessageType = NPL.load("(gl)script/Raft/RaftMessageType.lua");

local PeerServer = commonlib.gettable("Raft.PeerServer");

function PeerServer:new(server, ctx, heartbeatTimeoutHandler) 
    local o = {
        clusterConfig = server,
        rpcClient = nil,
        currentHeartbeatInterval = ctx.raftParameters.heartbeatInterval,
        heartbeatInterval = ctx.raftParameters.heartbeatInterval,
        rpcBackoffInterval = ctx.raftParameters.rpcFailureBackoff,
        maxHeartbeatInterval = ctx.raftParameters:getMaxHeartbeatInterval(),
        -- atomic
        busyFlag = 0,
        -- atomic
        pendingCommitFlag = 0,
        heartbeatTimeoutHandler = heartbeatTimeoutHandler,
        nextLogIndex = 0,
        matchedIndex = 0,
        heartbeatEnabled = false,
    };

    o.heartbeatTask = function(timer) o.heartbeatTimeoutHandler(o) end;
    o.heartbeatTimer = commonlib.Timer:new({callbackFunc = o.heartbeatTask})
    -- util.table_print(o)
    setmetatable(o, self);
    return o;
end

function PeerServer:__index(name)
    return rawget(self, name) or PeerServer[name];
end

function PeerServer:__tostring()
    return util.table_tostring(self);
end


function PeerServer:toBytes()
    return ;
end


-- make sure this happens in one NPL thread(state)
function PeerServer:setFree()
   self.busyFlag = 0;
end

function PeerServer:makeBusy()
    -- return self.busyFlag.compareAndSet(0, 1);
    if self.busyFlag == 0 then
        -- body
        self.busyFlag = 1;
        return true;
    end
    return false;
end


-- make sure this happens in one NPL thread(state)
function PeerServer:setPendingCommit()
   self.pendingCommitFlag = 1;
end

function PeerServer:clearPendingCommit()
    -- return this.pendingCommitFlag.compareAndSet(1, 0);
    if self.pendingCommitFlag == 1 then
        -- body
        self.pendingCommitFlag = 0;
        return true;
    end
    return false;
end


function PeerServer:getId()
    return self.clusterConfig.id;
end


function PeerServer:SendRequest(request, callbackFunc)
    local isAppendRequest = request.messageType == RaftMessageType.AppendEntriesRequest or
                      request.messageType == RaftMessageType.InstallSnapshotRequest;

    -- RaftRequestRPC is init in the RpcListener, suppose we could directly use here
    local o = self

    if (RaftRequestRPC("server"..request.source..":", "server"..request.destination..":", request, function(err, msg)
                    --    LOG.std(nil, "debug", "RaftResponseRPC", msg);
                       o:resumeHeartbeatingSpeed();

                       if callbackFunc then
                           callbackFunc(msg, err)
                       end
                   end) ~= 0) then
        
        -- TODO: re send 
        self:slowDownHeartbeating()
    end
    
    if(isAppendRequest) then
        self:setFree();
    end
end




function PeerServer:slowDownHeartbeating()
    self.currentHeartbeatInterval = math.min(self.maxHeartbeatInterval, self.currentHeartbeatInterval + self.rpcBackoffInterval);
end

function PeerServer:resumeHeartbeatingSpeed()
    if(self.currentHeartbeatInterval > self.heartbeatInterval) then
        self.currentHeartbeatInterval = self.heartbeatInterval;
    end
end