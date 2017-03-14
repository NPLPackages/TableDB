--[[
Title: 
Author: 
Date: 
Desc: 

Peer server in the same cluster for local server
this represents a peer for local server, it could be a leader, however, if local server is not a leader, though it has a list of peer servers, they are not used

------------------------------------------------------------
NPL.load("(gl)script/Raft.PeerServer.lua");
local PeerServer = commonlib.gettable("Raft.PeerServer");
------------------------------------------------------------
]]--

local RaftMessageType = NPL.load("(gl)script/Raft.RaftMessageType.lua");

local PeerServer = commonlib.gettable("Raft.PeerServer");

function PeerServer:new(server, ctx, heartbeatTimeoutHandler) 
    local o = {
        clusterConfig = server,
        rpcClient = nil,
        currentHeartbeatInterval = 0,
        heartbeatInterval = 0,
        rpcBackoffInterval = 0,
        maxHeartbeatInterval = 0,
        -- atomic
        busyFlag = 0,
        -- atomic
        pendingCommitFlag = 0,
        heartbeatTimeoutHandler = heartbeatTimeoutHandler,
        heartbeatTask = nil,
        nextLogIndex = 0,
        matchedIndex = 0,
        heartbeatEnabled = 0,
    };
    setmetatable(o, self);
    return o;
end

function PeerServer:__index(name)
    return rawget(self, name) or PeerServer[name];
end

function PeerServer:__tostring()
    return format("PeerServer(term:%d,commitIndex:%d,votedFor:%d)", self.term, self.commitIndex, self.votedFor);
end


function PeerServer:toBytes()
    return ;
end


function PeerServer:getId()
    return self.clusterConfig.id;
end


function PeerServer:SendRequest(request, callbackFunc)
    isAppendRequest = request.messageType == RaftMessageType.AppendEntriesRequest or
                      request.messageType == RaftMessageType.InstallSnapshotRequest;



    return ;
end