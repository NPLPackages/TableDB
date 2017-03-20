--[[
Title: 
Author: 
Date: 
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/RaftServer.lua");
local RaftServer = commonlib.gettable("Raft.RaftServer");
------------------------------------------------------------
]]--

NPL.load("(gl)script/ide/commonlib.lua");
NPL.load("(gl)script/Raft/RaftMessageSender.lua");
local RaftMessageSender = commonlib.gettable("Raft.RaftMessageSender");
NPL.load("(gl)script/Raft/ServerState.lua");
local ServerState = commonlib.gettable("Raft.ServerState");

local ServerRole = NPL.load("(gl)script/Raft/ServerRole.lua");
NPL.load("(gl)script/Raft/PeerServer.lua");
local PeerServer = commonlib.gettable("Raft.PeerServer");
NPL.load("(gl)script/ide/timer.lua");
local RaftMessageType = NPL.load("(gl)script/Raft/RaftMessageType.lua");



local RaftServer = commonlib.gettable("Raft.RaftServer");


function RaftServer:new(ctx) 
    local o = {
        context = ctx,
        id = ctx.serverStateManager.serverId,
        state = ctx.serverStateManager:readState(),
        logStore = ctx.serverStateManager.logStore,
        config = ctx.serverStateManager:loadClusterConfiguration(),
        stateMachine = ctx.stateMachine,
        votesGranted = 0,
        votesResponded = 0,
        leader = -1,
        electionCompleted = false,
        snapshotInProgress = 0, --atomic
        role = ServerRole.Follower,
        peers = {},
        logger = ctx.loggerFactory.getLogger("RaftServer"),
        
        -- fields for extended messages
        serverToJoin = nil;
        configChanging = false;
        catchingUp = false;
        steppingDown = 0;
        snapshotInProgress = 0;
        -- end fields for extended messages
    };
    if not o.state then
        o.state = ServerState:new()
    end

    o.electionTimer = commonlib.Timer:new({callbackFunc = function(timer)
                                               o:handleElectionTimeout()
                                           end}),
    setmetatable(o, self);

    for _,server in ipairs(o.config.servers) do
        if server.id ~= o.id then
            table.insert(o.peers, server.id, PeerServer:new(server, o.context,
                                                            function (peerServer)
                                                                util.table_print(peerServer)
                                                                o:handleHeartbeatTimeout(peerServer)
                                                            end))
        end
    end
    o.quickCommitIndex = o.state.commitIndex;

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
    return util.table_tostring(self)
end


function RaftServer:createMessageSender()
    return RaftMessageSender:new(self)
end

function RaftServer:processRequest(request)
    local entriesLength = 0
    if request.logEntries ~= nil then
        entriesLength = #request.logEntries
    end

    self.logger.debug("Receive a %s message from %d with LastLogIndex=%d, LastLogTerm=%d, EntriesLength=%d, CommitIndex=%d and Term=%d",
            request.messageType.string,
            request.source,
            request.lastLogIndex,
            request.lastLogTerm,
            entriesLength,
            request.commitIndex or 0,
            request.term);
    response = nil;
    if(request.messageType.int == RaftMessageType.AppendEntriesRequest.int) then
        response = self:handleAppendEntriesRequest(request);
    elseif(request.messageType.int == RaftMessageType.RequestVoteRequest.int) then
        response = self:handleVoteRequest(request);
    elseif(request.messageType.int == RaftMessageType.ClientRequest.int) then
        response = self:handleClientRequest(request);
    else
        -- extended requests
        response = self:handleExtendedMessages(request);
    end
    if(response ~= nil) then
        self.logger.debug("Response back a %s message to %d with Accepted=%s, Term=%d, NextIndex=%d",
                response.messageType.string,
                response.destination,
                (response.accepted and "true") or "false",
                response.term,
                response.nextIndex);
    end
    return response;
end

-- synchronized
function RaftServer:handleAppendEntriesRequest(request)
    -- we allow the server to be continue after term updated to save a round message
    self:updateTerm(request.term);
    -- Reset stepping down value to prevent self server goes down when leader crashes after sending a LeaveClusterRequest
    if(self.steppingDown > 0) then
        self.steppingDown = 2;
    end
    
    if(request.term == self.state.term) then
        if(self.role == ServerRole.Candidate) then
            self:becomeFollower();
        elseif(self.role == ServerRole.Leader) then
            self.logger.error("Receive AppendEntriesRequest from another leader(%d) with same term, there must be a bug, server exits", request.source);
            self.stateMachine.exit(-1);
        else
            self:restartElectionTimer();
        end
    end
    response = {
        messageType = RaftMessageType.AppendEntriesResponse,
        term = self.state.term,
        source = self.id,
        destination = request.destination,
    }

    -- After a snapshot the request.getLastLogIndex() may less than logStore.getStartingIndex() but equals to logStore.getStartingIndex() -1
    -- In self case, log is Okay if request.getLastLogIndex() == lastSnapshot.getLastLogIndex() and request.getLastLogTerm() == lastSnapshot.getLastTerm()
    logOkay = request.lastLogIndex == 0 or
            (request.lastLogIndex < self.logStore:getFirstAvailableIndex() and
                    request.getLastLogTerm() == self:termForLastLog(request.lastLogIndex));
    if(request.term < self.state.term or (not logOkay) )then
        response.accepted = false;
        response.nextIndex= self.logStore:getFirstAvailableIndex();
        return response;
    end
    -- The role is Follower and log is okay now
    if(request.logEntries ~= nil and #request.logEntries > 0) then
        -- write the logs to the store, first of all, check for overlap, and skip them
        logEntries = request.logEntries;
        index = request.lastLogIndex + 1;
        logIndex = 0;
        while(index < self.logStore:getFirstAvailableIndex() and
                logIndex < #logEntries and
                logEntries[logIndex].term == self.logStore:getLogEntryAt(index).term) do
            logIndex = logIndex + 1;
            index = index + 1;
        end
        -- dealing with overwrites
        while(index < self.logStore:getFirstAvailableIndex() and logIndex < #logEntries) do
            oldEntry = self.logStore:getLogEntryAt(index);
            if(oldEntry.getValueType() == LogValueType.Application) then
                self.stateMachine.rollback(index, oldEntry.getValue());
            elseif(oldEntry.getValueType() == LogValueType.Configuration) then
                self.logger.info("revert a previous config change to config at %d", self.config.getLogIndex());
                self.configChanging = false;
            end
            self.logStore:writeAt(index, logEntries[logIndex]);
            logIndex = logIndex + 1;
            index = index + 1;
        end
        -- append the new log entries
        while(logIndex < #logEntries) do
            logEntry = logEntries[logIndex];
            logIndex = logIndex + 1;
            indexForEntry = self.logStore:append(logEntry);
            if(logEntry.getValueType() == LogValueType.Configuration) then
                self.logger.info("received a configuration change at index %d from leader", indexForEntry);
                self.configChanging = true;
            else
                self.stateMachine.preCommit(indexForEntry, logEntry.getValue());
            end
        end
    end
    self.leader = request.source;
    self.commit(request.commitIndex);
    response.accepted = true;
    response.setNextIndex(request.lastLogIndex + ((request.logEntries == nil and 0 ) or #request.logEntries) + 1);
    return response;
end

-- synchronized
function RaftServer:handleVoteRequest(request)
    -- we allow the server to be continue after term updated to save a round message
    self:updateTerm(request.term);
    -- Reset stepping down value to prevent this server goes down when leader crashes after sending a LeaveClusterRequest
    if(self.steppingDown > 0) then
        self.steppingDown = 2;
    end
    
    response = {
        messageType = RaftMessageType.RequestVoteResponse,
        source=self.id,
        destination = request.source,
        term = self.state.term,
        nextIndex = 0,
    }
    logOkay = request.lastLogTerm > self.logStore:getLastLogEntry().term or
            (request.lastLogTerm == self.logStore:getLastLogEntry().term and
             self.logStore:getFirstAvailableIndex() - 1 <= request.lastLogIndex);
    grant = request.term == self.state.term and logOkay and (self.state.votedFor == request.source or self.state.votedFor == -1);
    response.accepted = grant;
    if(grant) then
        self.state.votedFor= request.source;
        self.context.serverStateManager:persistState(self.state);
    end
    return response;
end



function RaftServer:handleHeartbeatTimeout(peer)
    self.logger.debug("Heartbeat timeout for %d", peer:getId());
    if(self.role == ServerRole.Leader) then
        self:requestAppendEntries(peer);
        -- synchronized(peer){
        if(peer.heartbeatEnabled) then
            -- Schedule another heartbeat if heartbeat is still enabled
            peer.heartbeatTimer:Change(peer.currentHeartbeatInterval, nil);
        else
            self.logger.debug("heartbeat is disabled for peer %d", peer:getId());
        end
    else
        self.logger.info("Receive a heartbeat event for %d while no longer as a leader", peer:getId());
    end
end


function RaftServer:requestAllAppendEntries()
    if(#self.peers == 0) then
        self:commit(self.logStore:getFirstAvailableIndex() - 1);
        return;
    end

    for _,peer in ipairs(self.peers) do
        self:requestAppendEntries(peer);
    end
end

function RaftServer:requestAppendEntries(peer)
    if(peer:makeBusy()) then
        local o = self
        peer:SendRequest(self:createAppendEntriesRequest(peer), function (response)
            o:handlePeerResponse(response);
        end)
        return true;
    end

    self.logger.debug("Server %d is busy, skip the request", peer:getId());
    return false;
end


function RaftServer:createAppendEntriesRequest(peer)
    lastLogIndex = 0;
    -- synchronized(this){
    startingIndex = self.logStore:getStartIndex();
    currentNextIndex = self.logStore:getFirstAvailableIndex();
    commitIndex = self.quickCommitIndex;
    term = self.state.term;
    -- }
    -- synchronized(peer){
    if(peer.getNextLogIndex() == 0) then
        peer.setNextLogIndex(currentNextIndex);
    end
    lastLogIndex = peer.getNextLogIndex() - 1;
    -- }
    if(lastLogIndex >= currentNextIndex) then
        self.logger.error("Peer's lastLogIndex is too large %d v.s. %d, server exits", lastLogIndex, currentNextIndex);
        self.stateMachine.exit(-1);
    end
    -- -- for syncing the snapshots, if the lastLogIndex == lastSnapshot.getLastLogIndex, we could get the term from the snapshot
    -- if(lastLogIndex > 0 and lastLogIndex < startingIndex - 1) then
    --     return self:createSyncSnapshotRequest(peer, lastLogIndex, term, commitIndex);
    -- end
    lastLogTerm = self:termForLastLog(lastLogIndex);
    endIndex = math.min(currentNextIndex, lastLogIndex + 1 + context.getRaftParameters().getMaximumAppendingSize());
    logEntries = self.logStore:getLogEntries(lastLogIndex + 1, endIndex);
    if not ((lastLogIndex + 1) >= endIndex) then
         logEntries = nil
    end
    self.logger.debug(
            "An AppendEntries Request for %d with LastLogIndex=%d, LastLogTerm=%d, EntriesLength=%d, CommitIndex=%d and Term=%d",
            peer:getId(),
            lastLogIndex,
            lastLogTerm,
            (logEntries == nil and 0 ) or #logEntries,
            commitIndex,
            term);
    requestMessage = {
        messageType = RaftMessageType.AppendEntriesRequest,
        source = self.id,
        destination = peer:getId(),
        lastLogIndex = lastLogIndex,
        logEntries = logEntries,
        commitIndex = commitIndex,
        term = term,
    }
    return requestMessage;
end


function RaftServer:handleElectionTimeout()
    if(self.steppingDown > 0) then
        self.steppingDown = self.steppingDown - 1
        if(self.steppingDown == 0) then
            self.logger.info("no hearing further news from leader, remove this server from config and step down");
            server = self.config:getServer(self.id);
            if(server ~= nil) then
                self.config.servers[server] = nil;
                self.context.serverStateManager.saveClusterConfiguration(self.config);
            end
            
            self.stateMachine.exit(0);
            return;
        end

        self.logger.info("stepping down (cycles left: %d), skip this election timeout event", self.steppingDown);
        self:restartElectionTimer();
        return;
    end

    if(self.catchingUp) then
        -- this is a new server for the cluster, will not send out vote request until the config that includes this server is committed
        self.logger.info("election timeout while joining the cluster, ignore it.");
        self:restartElectionTimer();
        return;
    end


    if(self.role == ServerRole.Leader) then
        self.logger.error("A leader should never encounter election timeout, illegal application state, stop the application");
        self.stateMachine:exit(-1);
        return;
    end

    self.logger.debug("Election timeout, change to Candidate");
    self.state:increaseTerm();
    self.state.votedFor = -1;
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
    -- don't start the election timer while this server is still catching up the logs
    if(self.catchingUp) then
        return;
    end

    -- do not need to kill timer
    if self.electionTimer:IsEnabled() then
        self.electionTimer:Change()
    end
    
    raftParameters = self.context.raftParameters
    delta = raftParameters.electionTimeoutUpperBound - raftParameters.electionTimeoutLowerBound
    electionTimeout = raftParameters.electionTimeoutLowerBound + math.random(0, delta)
    self.electionTimer:Change(electionTimeout, nil)
end



function RaftServer:requestVote()
    -- vote for self
    self.logger.info("requestVote started with term %d", self.state.term);
    self.state.votedFor = self.id;
    self.context.serverStateManager:persistState(self.state);
    self.votesGranted = self.votesGranted + 1;
    self.votesResponded = self.votesResponded + 1;

    -- this is the only server?
    if(self.votesGranted > (#self.peers + 1) / 2) then
        self.electionCompleted = true;
        self:becomeLeader();
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

        self.logger.debug("send %s to server %d with term %d", RaftMessageType.RequestVoteRequest.string, peer:getId(), self.state.term);
        local o = self
        peer:SendRequest(request, function (response)
            o:handlePeerResponse(response);
        end);
    end
end


--synchronized
function RaftServer:handlePeerResponse(response)
    self.logger.debug(
            "Receive a %s message from peer %d with Result=%s, Term=%d, NextIndex=%d",
            response.messageType.string,
            response.source,
            (response.accepted and "true") or "false",
            response.term,
            response.nextIndex);
    -- If term is updated no need to proceed
    if(self:updateTerm(response.term)) then
        return;
    end

    -- Ignore the response that with lower term for safety
    if(response.term < self.state.term) then
        self.logger.info("Received a peer response from %d that with lower term value %d v.s. %d", response.source, response.term, self.state.term);
        return;
    end

    if(response.messageType.int == RaftMessageType.RequestVoteResponse.int) then
        self:handleVotingResponse(response);
    elseif(response.messageType.int == RaftMessageType.AppendEntriesResponse.int) then
        self:handleAppendEntriesResponse(response);
    elseif(response.messageType.int == RaftMessageType.InstallSnapshotResponse.int) then
        self:handleInstallSnapshotResponse(response);
    else
        self.logger.error("Received an unexpected message %s for response, system exits.", response.messageType.string);
        self.stateMachine.exit(-1);
    end
end


function RaftServer:handleVotingResponse(response)
    self.votesResponded = self.votesResponded + 1;
    if(self.electionCompleted) then
        self.logger.info("Election completed, will ignore the voting result from this server");
        return;
    end

    if(response.accepted) then
        self.votesGranted = self.votesGranted + 1;
    end

    if(self.votesResponded >= #self.peers + 1) then
        self.electionCompleted = true;
    end

    -- got a majority set of granted votes
    if(self.votesGranted > (#self.peers + 1) / 2) then
        self.logger.info("Server is elected as leader for term %d", self.state.term);
        self.electionCompleted = true;
        self:becomeLeader();
    end
end


function RaftServer:stopElectionTimer()
    if not self.electionTimer:IsEnabled() then
        self.logger.error("Election Timer is never started but is requested to stop, protential a bug");
    end

    self.electionTimer:Change()
end

function RaftServer:becomeLeader()
    self:stopElectionTimer();
    self.role = ServerRole.Leader;
    self.leader = self.id;
    self.serverToJoin = nil;
    for _,server in ipairs(self.peers) do
        server.nextLogIndex= self.logStore:getFirstAvailableIndex();
        server.snapshotInSync = nil;
        server:setFree();
        self:enableHeartbeatForPeer(server);
    end

    -- if current config is not committed, try to commit it
    if(self.config.logIndex == 0) then
        self.config.logIndex= self.logStore:getFirstAvailableIndex();
        -- self.logStore:append(LogEntry:new(self.state.term, self.config:toBytes(), LogValueType.Configuration));
        self.logger.info("add initial configuration to log store");
        self.configChanging = true;
    end

    self:requestAllAppendEntries();
end

function RaftServer:enableHeartbeatForPeer(peer)
    peer.heartbeatEnabled = true;
    peer:resumeHeartbeatingSpeed();
    peer.heartbeatTimer:Change(peer.currentHeartbeatInterval, nil);
end

function RaftServer:becomeFollower()
    -- stop heartbeat for all peers
    for _, server in ipairs(self.peers) do
        if server.heartbeatTimer:IsEnabled() then
            server.heartbeatTimer:Change()
        end
        server.heartbeatEnabled = false;
    end
    self.serverToJoin = nil;
    self.role = ServerRole.Follower;
    self:restartElectionTimer();
end

function RaftServer:updateTerm(term)
    if(term > self.state.term) then
        self.state.term = term;
        self.state.votedFor= -1;
        self.electionCompleted = false;
        self.votesGranted = 0;
        self.votesResponded = 0;
        self.context.serverStateManager:persistState(self.state);
        self:becomeFollower();
        return true;
    end

    return false;
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