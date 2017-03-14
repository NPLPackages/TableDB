--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft.RaftServer.lua");
local RaftServer = commonlib.gettable("Raft.RaftServer");
------------------------------------------------------------
]]--

NPL.load("(gl)script/Raft.ServerState.lua");
local ServerState = commonlib.gettable("Raft.ServerState");
NPL.load("(gl)script/Raft.ServerRole.lua");
local ServerRole = commonlib.gettable("Raft.ServerRole");
NPL.load("(gl)script/Raft.PeerServer.lua");
local ServerState = commonlib.gettable("Raft.PeerServer");
NPL.load("(gl)script/ide/timer.lua");
local RaftMessageType = NPL.load("(gl)script/Raft.RaftMessageType.lua");



local RaftServer = commonlib.gettable("Raft.RaftServer");


function RaftServer:new(ctx) 
    local o = {
        contex = ctx,
        id = ctx.serverStateManager.serverStateManager.getServerId,
        state = ctx.serverStateManager.serverStateManager.readState(),
        logStore = ctx.serverStateManager.serverStateManager.loadLogStore(),
        config = ctx.serverStateManager.serverStateManager.loadClusterConfiguration(),
        stateMachine = ctx.stateMachine,
        votesGranted = 0,
        votesResponded = 0,
        leader = -1,
        electionCompleted = false,
        snapshotInProgress = 0, --atomic need lock??
        role = ServerRole.Follower,
        peers = {},
        logger = commonlib.logging.GetLogger(""),
        electionTimer = commonlib.Timer:new({callbackFunc = function(timer)
            RaftServer:handleElectionTimeout()
        end})
    };
    if not o.state then
        o.state = ServerState:new()
    end
    setmetatable(o, self);

    for _,server in ipairs(o.config.servers) do
        if server.id ~= o.id then
            table.insert(o.peers, server.id, PeerServer:new(server, o.context,
            function (peerServer)
                o:handleHeartbeatTimeout(peerServer)
            end))
        end
    end
    -- o.quickCommitIndex = o.state.getCommitIndex();

    -- dedicated commit thread
     o.commitingThreadName = "commitingThread"..o.id;
     NPL.CreateRuntimeState(o.commitingThreadName, 0):Start();
     NPL.activate(format("(%s)script/Raft/RaftServer.lua", o.commitingThreadName),{
         server = o,
     });

    -- election timer
    o:restartElectionTimer()

    o.logger.info(format("Server %d started", o.id))
    return o;
end

function RaftServer:__index(name)
    return rawget(self, name) or RaftServer[name];
end

function RaftServer:__tostring()
    -- return format("RaftServer(term:%d,commitIndex:%d,votedFor:%d)", self.term, self.commitIndex, self.votedFor);
end

function RaftServer:handleHeartbeatTimeout(peer)
    -- body
end

function RaftServer:handleElectionTimeout()
    -- TODO: add extra

    if(self.role == ServerRole.Leader) then
        self.logger.error("A leader should never encounter election timeout, illegal application state, stop the application");
        self.stateMachine:exit(-1);
        return;
    end

    self.logger.debug("Election timeout, change to Candidate");
    self.state:increaseTerm();
    self.state:setVotedFor(-1);
    self.role = ServerRole.Candidate;
    self.votesGranted = 0;
    self.votesResponded = 0;
    self.electionCompleted = false;
    self.context.serverStateManager:persistState(self.state);
    self:requestVote();

    -- restart the election timer if this is not yet a leader
    if(self.role ~= ServerRole.Leader) then
        self:restartElectionTimer();
    end
end

function RaftServer:restartElectionTimer()
    raftParameters = self.context.raftParameters
    delta = raftParameters.electionTimeoutUpperBound - raftParameters.electionTimeoutLowerBound
    electionTimeout = raftParameters.electionTimeoutLowerBound + math.random(0, delta)
    self.electionTimer:Change(0, electionTimeout)
end


function RaftServer:requestVote()
    -- vote for self
    self.logger.info("requestVote started with term %d", self.state.getTerm());
    self.state.setVotedFor(self.id);
    self.context.serverStateManager():persistState(self.state);
    self.votesGranted = self.votesGranted + 1;
    self.votesResponded = self.votesResponded + 1;

    -- this is the only server?
    if(self.votesGranted > (self.peers.size() + 1) / 2) then
        self.electionCompleted = true;
        self.becomeLeader();
        return;
    end

    for _,peer in pairs(self.peers) do
        request = {
            messageType = RaftMessageType.RequestVoteRequest,
            destination = peer:getId(),
            source = self.id,
            lastLogIndex = self.logStore:getFirstAvailableIndex() - 1,
            lastLogTerm = self:termForLastLog(self.logStore:getFirstAvailableIndex() - 1),
            term = self.state.term,
        }

        self.logger.debug("send %s to server %d with term %d", RaftMessageType.RequestVoteRequest, peer.getId(), self.state.term);
        peer:SendRequest(request, function (response, error)
            handlePeerResponse(response, error);
        end);
    end
end




function RaftServer:termForLastLog(logIndex)
    if(logIndex == 0) then
        return 0;
    end

    if(logIndex >= self.logStore:getStartIndex()) then
        return self.logStore:getLogEntryAt(logIndex):getTerm();
    end
    lastSnapshot = self.stateMachine:getLastSnapshot();
    if(lastSnapshot == nil or logIndex ~= lastSnapshot:getLastLogIndex()) then
        self.logger.error("logIndex is beyond the range that no term could be retrieved");
    end
    return lastSnapshot.getLastLogTerm();
end





-- TODO: resemble IOThread in TableDB

-- commiting thread
local function activate()
   if(msg and msg.server) then
        local server = msg.server
        -- commit

   end
end

NPL.this(activate);