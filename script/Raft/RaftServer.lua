--[[
Title: 
Author: liuluheng
Date: 2017.03.25
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
NPL.load("(gl)script/Raft/LogEntry.lua");
local LogEntry = commonlib.gettable("Raft.LogEntry");
local LogValueType = NPL.load("(gl)script/Raft/LogValueType.lua");
local ServerRole = NPL.load("(gl)script/Raft/ServerRole.lua");
NPL.load("(gl)script/Raft/PeerServer.lua");
local PeerServer = commonlib.gettable("Raft.PeerServer");
NPL.load("(gl)script/ide/timer.lua");
local RaftMessageType = NPL.load("(gl)script/Raft/RaftMessageType.lua");
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)script/Raft/ClusterConfiguration.lua");
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration");
NPL.load("(gl)script/Raft/Rutils.lua");
local Rutils = commonlib.gettable("Raft.Rutils");

local RaftServer = commonlib.gettable("Raft.RaftServer");


local indexComparator = function (arg0, arg1)
    -- body
    return arg0 > arg1;
end

local real_commit -- 'forward' declarations

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
            local peer = PeerServer:new(server, o.context,function (s)
                                                        o:handleHeartbeatTimeout(s)
                                                    end)
            o.peers[server.id] = peer
        end
    end
    o.quickCommitIndex = o.state.commitIndex;
    -- FIXME: why???
    -- what's wrong with o.logger.debug
    -- o.logger.debug(o.peers)

    -- dedicated commit thread
    --  o.commitingThreadName = "commitingThread"..o.id;
    --  NPL.CreateRuntimeState(o.commitingThreadName, 0):Start();
    --  NPL.activate(format("(%s)script/Raft/RaftServer.lua", o.commitingThreadName),{
    --      server = o,
    --  });

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
            request.source or -1,
            request.lastLogIndex or -1,
            request.lastLogTerm or -1,
            entriesLength,
            request.commitIndex or 0,
            request.term or -1);
    
    local response;
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
    -- Reset stepping down value to prevent this server goes down when leader crashes after sending a LeaveClusterRequest
    if(self.steppingDown > 0) then
        self.steppingDown = 2;
    end
    
    if(request.term == self.state.term) then
        if(self.role == ServerRole.Candidate) then
            self:becomeFollower();
        elseif(self.role == ServerRole.Leader) then
            self.logger.error("Receive AppendEntriesRequest from another leader(%d) with same term, there must be a bug, server exits", request.source);
            self.stateMachine:exit(-1);
        else
            self:restartElectionTimer();
        end
    end
    local response = {
        messageType = RaftMessageType.AppendEntriesResponse,
        term = self.state.term,
        source = self.id,
        destination = request.source,
    }

    -- After a snapshot the request.lastLogIndex may less than logStore:getStartingIndex() but equals to logStore:getStartingIndex() -1
    -- In this case, log is Okay if request.lastLogIndex == lastSnapshot.lastLogIndex and request.lastLogTerm == lastSnapshot.getLastTerm()
    local logOkay = request.lastLogIndex == 0 or
            (request.lastLogIndex < self.logStore:getFirstAvailableIndex() and
                    request.lastLogTerm == self:termForLastLog(request.lastLogIndex));
    if(request.term < self.state.term or (not logOkay) )then
        response.accepted  = false;
        response.nextIndex = self.logStore:getFirstAvailableIndex();
        return response;
    end
    -- The role is Follower and log is okay now
    if(request.logEntries ~= nil and #request.logEntries > 0) then
        -- write the logs to the store, first of all, check for overlap, and skip them
        local logEntries = request.logEntries;
        local index = request.lastLogIndex + 1;
        local logIndex = 1;
        self.logger.trace("initial->entry len:%d, index:%d, logIndex:%d", #logEntries, index, logIndex)
        while(index < self.logStore:getFirstAvailableIndex() and
                logIndex < #logEntries + 1 and
                logEntries[logIndex].term == self.logStore:getLogEntryAt(index).term) do
            logIndex = logIndex + 1;
            index = index + 1;
        end
        self.logger.trace("checked overlap->entry len:%d, index:%d, logIndex:%d", #logEntries, index, logIndex)
        -- dealing with overwrites
        while(index < self.logStore:getFirstAvailableIndex() and logIndex < #logEntries + 1) do
            local oldEntry = self.logStore:getLogEntryAt(index);
            if(oldEntry.valueType == LogValueType.Application) then
                self.stateMachine:rollback(index, oldEntry.value);
            elseif(oldEntry.valueType == LogValueType.Configuration) then
                self.logger.info("revert a previous config change to config at %d", self.config.logIndex);
                self.configChanging = false;
            end
            self.logStore:writeAt(index, logEntries[logIndex]);
            logIndex = logIndex + 1;
            index = index + 1;
        end
        self.logger.trace("dealed overwrite->entry len:%d, index:%d, logIndex:%d", #logEntries, index, logIndex)
        -- append the new log entries
        while(logIndex < #logEntries + 1) do
            local logEntry = logEntries[logIndex];
            self.logger.trace("dealing with logEntry:"..util.table_tostring(logEntry))
            logIndex = logIndex + 1;
            local indexForEntry = self.logStore:append(logEntry);
            if(logEntry.valueType == LogValueType.Configuration) then
                self.logger.info("received a configuration change at index %d from leader", indexForEntry);
                self.configChanging = true;
            else
                self.stateMachine:preCommit(indexForEntry, logEntry.value);
            end
        end
    end
    self.leader = request.source;
    self:commit(request.commitIndex);
    response.accepted = true;
    response.nextIndex = request.lastLogIndex + ((request.logEntries == nil and 0 ) or #request.logEntries) + 1;
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
    
    local response = {
        messageType = RaftMessageType.RequestVoteResponse,
        source = self.id,
        destination = request.source,
        term = self.state.term,
        nextIndex = 0,
    }
    local logOkay = request.lastLogTerm > self.logStore:getLastLogEntry().term or
            (request.lastLogTerm == self.logStore:getLastLogEntry().term and
             self.logStore:getFirstAvailableIndex() - 1 <= request.lastLogIndex);
    local grant = request.term == self.state.term and logOkay and (self.state.votedFor == request.source or self.state.votedFor == -1);
    response.accepted = grant;
    if (grant) then
        self.state.votedFor = request.source;
        self.context.serverStateManager:persistState(self.state);
    end
    return response;
end

function RaftServer:handleClientRequest(request)
    local response = {
        messageType = RaftMessageType.AppendEntriesResponse,
        source = self.id,
        destination = self.leader, -- use destination to indicate the leadId
        term = self.state.term,
    }

    if self.role ~= ServerRole.Leader then
        response.accepted = false
        return response
    end

    local term = self.state.term

    if request.logEntries and #request.logEntries > 0 then

        for i,v in ipairs(request.logEntries) do
            local logEntry = LogEntry:new(term, v.value)
            local logIndex = self.logStore:append(logEntry)
            self.stateMachine:preCommit(logIndex, v.value);
        end
    end

    self:requestAllAppendEntries();
    response.accepted = true;
    response.nextIndex = self.logStore:getFirstAvailableIndex();
    return response
end

function RaftServer:handleElectionTimeout()
    if(self.steppingDown > 0) then
        self.steppingDown = self.steppingDown - 1
        if(self.steppingDown == 0) then
            self.logger.info("no hearing further news from leader, remove this server from config and step down");
            local server = self.config:getServer(self.id);
            if(server ~= nil) then
                self.config.servers[server.id] = nil;
                self.context.serverStateManager:saveClusterConfiguration(self.config);
            end
            
            self.stateMachine:exit(0);
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


function RaftServer:requestVote()
    -- vote for self
    self.logger.info("requestVote started with term %d", self.state.term);
    self.state.votedFor = self.id;
    self.context.serverStateManager:persistState(self.state);
    self.votesGranted = self.votesGranted + 1;
    self.votesResponded = self.votesResponded + 1;

    -- this is the only server?
    if(self.votesGranted > (Rutils.table_size(self.peers) + 1) / 2) then
        self.electionCompleted = true;
        self:becomeLeader();
        return;
    end

    for _,peer in pairs(self.peers) do
        local request = {
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


function RaftServer:requestAllAppendEntries()
    -- be careful with the table and sequence array !
    self.logger.warn("#self.peers:%d, peers table size:%d", #self.peers, Rutils.table_size(self.peers))
    if(Rutils.table_size(self.peers) == 0) then
        self:commit(self.logStore:getFirstAvailableIndex() - 1);
        return;
    end

    for _,peer in pairs(self.peers) do
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
        self.stateMachine:exit(-1);
    end
end


function RaftServer:handleAppendEntriesResponse(response)
    local peer = self.peers[response.source];
    if(peer == nil) then
        self.logger.info("the response is from an unkown peer %d", response.source);
        return;
    end

    -- If there are pending logs to be synced or commit index need to be advanced, continue to send appendEntries to this peer
    local needToCatchup = true;
    if(response.accepted) then
        -- synchronized(peer){
        peer.nextLogIndex = response.nextIndex;
        peer.matchedIndex = response.nextIndex - 1;
        -- }

        -- try to commit with this response
        local matchedIndexes = {};
        matchedIndexes[#matchedIndexes + 1] = self.logStore:getFirstAvailableIndex() - 1
        for _,p in pairs(self.peers) do
            matchedIndexes[#matchedIndexes + 1] = p.matchedIndex
        end

        self.logger.trace("all matchedIndexes:%s", util.table_tostring(matchedIndexes))
        table.sort(matchedIndexes, indexComparator);
        self.logger.trace("sorted matchedIndexes:%s", util.table_tostring(matchedIndexes))
        -- majority is a float without math.floor
        -- lua index start form 1
        local majority = math.ceil((Rutils.table_size(self.peers) + 1) / 2);
        self.logger.trace("total server num:%d, major num:%d", Rutils.table_size(self.peers) + 1, majority)
        self:commit(matchedIndexes[majority]);
        needToCatchup = peer:clearPendingCommit() or response.nextIndex < self.logStore:getFirstAvailableIndex();
    else
        -- synchronized(peer){
        -- Improvement: if peer's real log length is less than was assumed, reset to that length directly
        if(response.nextIndex > 0 and peer.nextLogIndex > response.nextIndex) then
            peer.nextLogIndex = response.nextIndex;
        else
            peer.nextLogIndex = peer.nextLogIndex - 1;
        end
        -- }
    end

    -- This may not be a leader anymore, such as the response was sent out long time ago
    -- and the role was updated by UpdateTerm call
    -- Try to match up the logs for this peer
    if(self.role == ServerRole.Leader and needToCatchup) then
        self:requestAppendEntries(peer);
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

    if(self.votesResponded >= Rutils.table_size(self.peers) + 1) then
        self.electionCompleted = true;
    end

    -- got a majority set of granted votes
    if(self.votesGranted > (Rutils.table_size(self.peers) + 1) / 2) then
        self.logger.info("Server is elected as leader for term %d", self.state.term);
        self.electionCompleted = true;
        self:becomeLeader();
    end
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

function RaftServer:restartElectionTimer()
    -- don't start the election timer while this server is still catching up the logs
    if(self.catchingUp) then
        return;
    end

    -- do not need to kill timer
    if self.electionTimer:IsEnabled() then
        self.electionTimer:Change()
    end
    
    local raftParameters = self.context.raftParameters
    local delta = raftParameters.electionTimeoutUpperBound - raftParameters.electionTimeoutLowerBound
    local electionTimeout = raftParameters.electionTimeoutLowerBound + math.random(0, delta)
    -- self.logger.debug("electionTimeout:%d,%d,%d,%d", electionTimeout, delta, raftParameters.electionTimeoutUpperBound, raftParameters.electionTimeoutLowerBound)
    self.electionTimer:Change(electionTimeout, nil)
end

function RaftServer:stopElectionTimer()
    if not self.electionTimer:IsEnabled() then
        self.logger.error("Election Timer is never started but is requested to stop, protential a bug");
    end

    self.electionTimer:Change()
    assert(not self.electionTimer:IsEnabled(), "stopElectionTimer error")
end

function RaftServer:becomeLeader()
    self:stopElectionTimer();
    self.role = ServerRole.Leader;
    self.leader = self.id;
    self.serverToJoin = nil;
    for _,server in pairs(self.peers) do
        server.nextLogIndex= self.logStore:getFirstAvailableIndex();
        server.snapshotInSync = nil;
        server:setFree();
        self:enableHeartbeatForPeer(server);
    end

    -- if current config is not committed, try to commit it
    if(self.config.logIndex == 0) then
        self.config.logIndex= self.logStore:getFirstAvailableIndex();
        self.logStore:append(LogEntry:new(self.state.term, self.config:toBytes(), LogValueType.Configuration));
        self.logger.info("add initial configuration to log store");
        self.configChanging = true;
    end

    self:requestAllAppendEntries();
end

function RaftServer:enableHeartbeatForPeer(peer)
    peer.heartbeatEnabled = true;
    peer:resumeHeartbeatingSpeed();
    peer.heartbeatTimer:Change(peer.currentHeartbeatInterval, nil);
   assert(peer.heartbeatTimer:IsEnabled(), "start heartbeatTimer error")
end

function RaftServer:becomeFollower()
    -- stop heartbeat for all peers
    for _, server in pairs(self.peers) do
        if server.heartbeatTimer:IsEnabled() then
            server.heartbeatTimer:Change()
            assert(not server.heartbeatTimer:IsEnabled(), "stop heartbeatTimer error")
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

function RaftServer:commit(targetIndex)
    self.logger.trace("committing..targetIndex:%d, quickCommitIndex:%d", targetIndex, self.quickCommitIndex)
    if(targetIndex > self.quickCommitIndex) then
        self.quickCommitIndex = targetIndex;

        -- if this is a leader notify peers to commit as well
        -- for peers that are free, send the request, otherwise, set pending commit flag for that peer
        if(self.role == ServerRole.Leader) then
            for _,peer in pairs(self.peers) do
                if( not self:requestAppendEntries(peer)) then
                    peer:setPendingCommit();
                end
            end
        end
    end

    if(self.logStore:getFirstAvailableIndex() - 1 > self.state.commitIndex and self.quickCommitIndex > self.state.commitIndex) then
        -- self.commitingThread.moreToCommit();
        real_commit(self);
    end
end



function RaftServer:createAppendEntriesRequest(peer)
    -- synchronized(this){
    local startingIndex = self.logStore:getStartIndex();
    local currentNextIndex = self.logStore:getFirstAvailableIndex();
    local commitIndex = self.quickCommitIndex;
    local term = self.state.term;
    -- }
    -- synchronized(peer){
    if(peer.nextLogIndex == 0) then
        peer.nextLogIndex = currentNextIndex;
    end
    
    local lastLogIndex = peer.nextLogIndex - 1;
    -- }
    if(lastLogIndex >= currentNextIndex) then
        self.logger.error("Peer's lastLogIndex is too large %d v.s. %d, server exits", lastLogIndex, currentNextIndex);
        self.stateMachine:exit(-1);
    end
    -- for syncing the snapshots, if the lastLogIndex == lastSnapshot.getLastLogIndex, we could get the term from the snapshot
    -- if(lastLogIndex > 0 and lastLogIndex < startingIndex - 1) then
    --     return self:createSyncSnapshotRequest(peer, lastLogIndex, term, commitIndex);
    -- end
    local lastLogTerm = self:termForLastLog(lastLogIndex);
    local endIndex = math.min(currentNextIndex, lastLogIndex + 1 + self.context.raftParameters.maximumAppendingSize);
    local logEntries;
    if not ((lastLogIndex + 1) >= endIndex) then
         logEntries = self.logStore:getLogEntries(lastLogIndex + 1, endIndex)
    end
    self.logger.trace("endIndex:%d, currentNextIndex:%d, max endIndex%d",
                       endIndex, currentNextIndex, lastLogIndex + 1 + self.context.raftParameters.maximumAppendingSize)
    self.logger.debug(
            "An AppendEntries Request for %d with LastLogIndex=%d, LastLogTerm=%d, EntriesLength=%d, CommitIndex=%d and Term=%d",
            peer:getId(),
            lastLogIndex,
            lastLogTerm,
            (logEntries == nil and 0) or #logEntries,
            commitIndex,
            term);
    local requestMessage = {
        messageType = RaftMessageType.AppendEntriesRequest,
        source = self.id,
        destination = peer:getId(),
        lastLogIndex = lastLogIndex,
        lastLogTerm = lastLogTerm,
        logEntries = logEntries,
        commitIndex = commitIndex,
        term = term,
    }
    return requestMessage;
end

-- private
function RaftServer:reconfigure(newConfig)
    self.logger.debug(
            "system is reconfigured to have %d servers, last config index: %d, this config index: %d",
            Rutils.table_size(newConfig.servers),
            newConfig.lastLogIndex,
            newConfig.logIndex);

    local serversRemoved = {}
    local serversAdded = {}

    for _,server in ipairs(newConfig.servers) do
        if not self.peers[server.id] and server.id ~= self.id then
            -- new add server
            serversAdded[server] = true;
        end
    end

    for id,_ in pairs(self.peers) do
        if newConfig:getServer(id) == nil then
            -- the server has been removed
            serversRemoved[id] = true
        end
    end

    if newConfig:getServer(self.id) == nil then
        serversRemoved[self.id] = true;
    end

    for server,_ in pairs(serversAdded) do
        if(server.id ~= self.id) then
           local o = self
           local peer = PeerServer:new(server, self.context, function (s)
                                                        o:handleHeartbeatTimeout(s)
                                                    end)
           peer.nextLogIndex = self.logStore:getFirstAvailableIndex();
           self.peers[server.id] = peer;
           self.logger.info("server %d is added to cluster", peer:getId());
           if(self.role == ServerRole.Leader) then
               self.logger.info("enable heartbeating for server %d", peer.getId());
               self:enableHeartbeatForPeer(peer);
               if(self.serverToJoin ~= nil and self.serverToJoin:getId() == peer:getId()) then
                   peer.nextLogIndex = self.serverToJoin.nextLogIndex;
                   self.serverToJoin = nil;
               end
           end
        end
    end

    for id,_ in pairs(serversRemoved) do
        if(id == self.id and not self.catchingUp) then
            -- this server is removed from cluster
            self.context.serverStateManager:saveClusterConfiguration(newConfig);
            self.logger.info("server has been removed from cluster, step down");
            self.stateMachine:exit(0);
            return;
        end
        
        local peer = self.peers[id];
        if(peer == nil) then
            self.logger.info("peer %d cannot be found in current peer list", id);
        else
            peer.heartbeatTimer:Change()
            peer.heartbeatEnabled = false;
            self.peers[id] = nil;
            self.logger.info("server %d is removed from cluster", id);
        end
    end

    self.config = newConfig;

end

function RaftServer:termForLastLog(logIndex)
    if(logIndex == 0) then
        return 0;
    end

    if(logIndex >= self.logStore:getStartIndex()) then
        return self.logStore:getLogEntryAt(logIndex).term;
    end
    local lastSnapshot = self.stateMachine:getLastSnapshot();
    if(lastSnapshot == nil or logIndex ~= lastSnapshot:getLastLogIndex()) then
        self.logger.error("logIndex is beyond the range that no term could be retrieved");
    end
    return lastSnapshot.lastLogTerm;
end


-- TODO: resemble IOThread in TableDB

function real_commit(server)
    local currentCommitIndex = server.state.commitIndex;
    while(currentCommitIndex < server.quickCommitIndex and currentCommitIndex < server.logStore:getFirstAvailableIndex() - 1) do
        currentCommitIndex = currentCommitIndex + 1;
        server.logger.trace("commiting...currentCommitIndex:%d, quickCommitIndex:%d, logStoreFirstAvailableIndex:%d",
                             currentCommitIndex, server.quickCommitIndex, server.logStore:getFirstAvailableIndex())
        local logEntry = server.logStore:getLogEntryAt(currentCommitIndex);

        -- FIXME:
        -- how can a LogEntry be nil ??
        -- perhaps on the start up, self commitIndex is -1, logStore's StartIndex is 1
        -- need more test here
        if logEntry == nil then
            -- do nothing
        elseif(logEntry.valueType == LogValueType.Application) then
            server.stateMachine:commit(currentCommitIndex, logEntry.value);
        elseif(logEntry.valueType == LogValueType.Configuration) then
            local newConfig = ClusterConfiguration:fromBytes(logEntry.value);
            server.logger.info("configuration at index %d is committed", newConfig.logIndex);
            server.context.serverStateManager:saveClusterConfiguration(newConfig);
            server.configChanging = false;
            if(server.config.logIndex < newConfig.logIndex) then
                server:reconfigure(newConfig);
            end
            
            if(server.catchingUp and newConfig:getServer(server.id) ~= nil) then
                server.logger.info("this server is committed as one of cluster members");
                server.catchingUp = false;
            end
        end

        server.state.commitIndex = currentCommitIndex;
        -- server:snapshotAndCompact(currentCommitIndex);
    end

    server.context.serverStateManager:persistState(server.state);
end


-- commiting thread
local function activate()
   if(msg and msg.server) then
        local server = msg.server
        -- commit

   end
end

NPL.this(activate);