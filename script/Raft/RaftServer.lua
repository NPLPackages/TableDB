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
NPL.load("(gl)script/Raft/SnapshotSyncRequest.lua");
local SnapshotSyncRequest = commonlib.gettable("Raft.SnapshotSyncRequest");
NPL.load("(gl)script/Raft/Rutils.lua");
local Rutils = commonlib.gettable("Raft.Rutils");

local RaftServer = commonlib.gettable("Raft.RaftServer");

local DEFAULT_SNAPSHOT_SYNC_BLOCK_SIZE = 4 * 1024;

local indexComparator = function (arg0, arg1)
    
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
            local peer = PeerServer:new(server, o.context, function (s)
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
        peer:SendRequest(request, function (response, error)
                                      o:handlePeerResponse(response, error);
                                  end);
    end
end


function RaftServer:requestAllAppendEntries()
    -- be careful with the table and sequence array !
    self.logger.trace("#self.peers:%d, peers table size:%d", #self.peers, Rutils.table_size(self.peers))
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
        peer:SendRequest(self:createAppendEntriesRequest(peer), function (response, error)
                                                                    o:handlePeerResponse(response, error);
                                                                end)
        return true;
    end

    self.logger.debug("Server %d is busy, skip the request", peer:getId());
    return false;
end


--synchronized
function RaftServer:handlePeerResponse(response, error)
    if(error ~= nil) then
        self.logger.info("peer response error: %s", error);
        return;
    end

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

function RaftServer:snapshotAndCompact(indexCommitted)
    local snapshotInAction = false;
    -- see if we need to do snapshots
    if(self.context.raftParameters.snapshotDistance > 0
        and ((indexCommitted - self.logStore:getStartIndex()) > self.context.raftParameters.snapshotDistance)
        and self.snapshotInProgress == 0) then
        self.snapshotInProgress = 1
        snapshotInAction = true;
        local currentSnapshot = self.stateMachine:getLastSnapshot();
        if(currentSnapshot ~= nil and indexCommitted - currentSnapshot.lastLogIndex < self.context.raftParameters.snapshotDistance) then
            self.logger.info("a very recent snapshot is available at index %d, will skip this one", currentSnapshot.lastLogIndex);
            self.snapshotInProgress = 0;
            snapshotInAction = false;
        else
            self.logger.info("creating a snapshot for index %d", indexCommitted);

            -- get the latest configuration info
            local config = self.config;
            while(config.logIndex > indexCommitted and config.lastLogIndex >= self.logStore:getStartIndex()) do
                config = ClusterConfiguration:fromBytes(self.logStore:getLogEntryAt(config.lastLogIndex).value);
            end

            if(config.logIndex > indexCommitted and config.lastLogIndex > 0 and config.lastLogIndex < self.logStore:getStartIndex()) then
                local lastSnapshot = self.stateMachine:getLastSnapshot();
                if(lastSnapshot == nil) then
                    self.logger.error("No snapshot could be found while no configuration cannot be found in current committed logs, this is a system error, exiting");
                    self.stateMachine:exit(-1);
                    return;
                end

                config = lastSnapshot.lastConfig;
            elseif(config.logIndex > indexCommitted and config.lastLogIndex == 0) then
                self.logger.error("BUG!!! stop the system, there must be a configuration at index one");
                self.stateMachine:exit(-1);
            end

            local indexToCompact = indexCommitted - 1;
            local logTermToCompact = self.logStore:getLogEntryAt(indexToCompact).term;
            local snapshot = Snapshot:new(indexToCompact, logTermToCompact, config);

            local o = self
            self.stateMachine:createSnapshot(snapshot, function (result, error)
                    if(error ~= nil) then
                        o.logger.error("failed to create a snapshot due to %s", error);
                        return;
                    end

                    if(not result) then
                        o.logger.info("the state machine rejects to create the snapshot");
                        return;
                    end

                    -- synchronized(this){
                    o.logger.debug("snapshot created, compact the log store");

                    o.logStore:compact(snapshot.lastLogIndex);
                    -- o.logger.error("failed to compact the log store, no worries, the system still in a good shape", ex);

                    o.snapshotInProgress = 0;
            end )
            snapshotInAction = false;
        end
    end
    -- error handle
    -- self.logger.error("failed to compact logs at index %d, due to errors %s", indexCommitted, error);
    -- if(snapshotInAction) then
    --     self.snapshotInProgress = 0;
    -- end
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

-- synchronized
function RaftServer:handleExtendedMessages(request)
    if(request.messageType.int == RaftMessageType.AddServerReques.int) then
        return self:handleAddServerRequest(request);
    elseif(request.messageType.int == RaftMessageType.RemoveServerRequest.int) then
        return self:handleRemoveServerRequest(request);
    elseif(request.messageType.int == RaftMessageType.SyncLogRequest.int) then
        return self:handleLogSyncRequest(request);
    elseif(request.messageType.int == RaftMessageType.JoinClusterRequest.int) then
        return self:handleJoinClusterRequest(request);
    elseif(request.messageType.int == RaftMessageType.LeaveClusterRequest.int) then
        return self:handleLeaveClusterRequest(request);
    elseif(request.messageType.int == RaftMessageType.InstallSnapshotRequest.int) then
        return self:handleInstallSnapshotRequest(request);
    else
        self.logger.error("receive an unknown request %s, for safety, step down.", request.messageType.string);
        self.stateMachine:exit(-1);
    end
end


-- synchronized
function RaftServer:handleExtendedResponse(response, error)
    if(error ~= nil) then
        self:handleExtendedResponseError(error);
        return;
    end

    self.logger.debug(
            "Receive an extended %s message from peer %d with Result=%s, Term=%d, NextIndex=%d",
            response.messageType.string,
            response.source,
            (response.accepted and "true") or "false",
            response.term,
            response.nextIndex);
    if(response.messageType.int == RaftMessageType.SyncLogResponse.int) then
        if(self.serverToJoin ~= nil) then
            -- we are reusing heartbeat interval value to indicate when to stop retry
            self.serverToJoin:resumeHeartbeatingSpeed();
            self.serverToJoin.nextLogIndex = response.nextIndex;
            self.serverToJoin.matchedIndex = response.nextIndex - 1;
            self:syncLogsToNewComingServer(response.nextIndex);
        end
    elseif(response.messageType.int == RaftMessageType.JoinClusterResponse.int) then
        if(self.serverToJoin ~= nil) then
            if(response.accepted) then
                self.logger.debug("new server confirms it will join, start syncing logs to it");
                self:syncLogsToNewComingServer(1);
            else
                self.logger.debug("new server cannot accept the invitation, give up");
            end
        else
            self.logger.debug("no server to join, drop the message");
        end
    elseif(response.messageType.int == RaftMessageType.LeaveClusterResponse.int) then
        if(not response.accepted) then
            self.logger.info("peer doesn't accept to stepping down, stop proceeding");
            return;
        end

        self.logger.debug("peer accepted to stepping down, removing this server from cluster");
        self:removeServerFromCluster(response.source);
    elseif(response.messageType.int == RaftMessageType.InstallSnapshotResponse.int) then
        if(self.serverToJoin == nil) then
            self.logger.info("no server to join, the response must be very old.");
            return;
        end

        if(not response.accepted) then
            self.logger.info("peer doesn't accept the snapshot installation request");
            return;
        end

        local context = self.serverToJoin:getSnapshotSyncContext();
        if(context == nil) then
            self.logger.error("Bug! SnapshotSyncContext must not be nil");
            self.stateMachine:exit(-1);
            return;
        end

        if(response.nextIndex >= context.snapshot.size) then
            -- snapshot is done
            self.logger.debug("snapshot has been copied and applied to new server, continue to sync logs after snapshot");
            self.serverToJoin:setSnapshotInSync(nil);
            self.serverToJoin.nextLogIndex = context.snapshot.lastLogIndex + 1;
            self.serverToJoin.matchedIndex = context.snapshot.lastLogIndex;
        else
            context.offset = response.nextIndex; 
            self.logger.debug("continue to send snapshot to new server at offset %d", response.nextIndex);
        end

        self:syncLogsToNewComingServer(self.serverToJoin.nextLogIndex);
    else
        -- No more response message types need to be handled
        self.logger.error("received an unexpected response message type %s, for safety, stepping down", response.messageType.string);
        self.stateMachine:exit(-1);
    end
end

function RaftServer:handleRemoveServerRequest(request)
    local logEntries = request.logEntries;
    local response = {
        source = self.id,
        destination = self.leader,
        term = self.state.term,
        messageType = RaftMessageType.AddServerResponse,
        nextIndex = self.logStore:getFirstAvailableIndex(),
        accepted = false,
    }

    if(#logEntries ~= 1 or logEntries[0].valueType ~= LogValueType.ClusterServer) then
        self.logger.info("bad add server request as we are expecting one log entry with value type of ClusterServer");
        return response;
    end

    if(self.role ~= ServerRole.Leader) then
        self.logger.info("this is not a leader, cannot handle RemoveServerRequest");
        return response;
    end

    if(self.configChanging) then
        -- the previous config has not committed yet
        self.logger.info("previous config has not committed yet");
        return response;
    end

    local server = ClusterServer:new(logEntries[0].value);
    if(self.peers[server.Id] or self.id == server.id) then
        self.logger.warning("the server to be added has a duplicated id with existing server %d", server.id);
        return response;
    end

    local serverId = tonumber(logEntries[0].value)
    if(serverId == self.id) then
        self.logger.info("cannot request to remove leader");
        return response;
    end

    local peer = self.peers[serverId];
    if(peer == nil) then
        self.logger.info("server %d does not exist", serverId);
        return response;
    end

    local leaveClusterRequest = {
        commitIndex = self.quickCommitIndex,
        destination = peer:getId(),
        lastLogIndex = self.logStore:getFirstAvailableIndex() - 1,
        lastLogTerm = 0,
        term = self.state.term,
        messageType = RaftMessageType.leaveClusterRequest,
        source = self.id,
    }

    local o = self
    peer:SendRequest(request, function (response, error)
                                  o:handleExtendedResponse(response, error);
                              end);

    response.accepted = true;
    return response;
end


function RaftServer:handleAddServerRequest(request)
    local logEntries = request.logEntries;
    local response = {
        source = self.id,
        destination = self.leader,
        term = self.state.term,
        messageType = RaftMessageType.AddServerResponse,
        nextIndex = self.logStore:getFirstAvailableIndex(),
        accepted = false,
    }

    if(#logEntries ~= 1 or logEntries[0].valueType ~= LogValueType.ClusterServer) then
        self.logger.info("bad add server request as we are expecting one log entry with value type of ClusterServer");
        return response;
    end

    if(self.role ~= ServerRole.Leader) then
        self.logger.info("this is not a leader, cannot handle AddServerRequest");
        return response;
    end

    local server = ClusterServer:new(logEntries[0].value);
    if(self.peers[server.Id] or self.id == server.id) then
        self.logger.warning("the server to be added has a duplicated id with existing server %d", server.id);
        return response;
    end

    if(self.configChanging) then
        -- the previous config has not committed yet
        self.logger.info("previous config has not committed yet");
        return response;
    end

    local o = self
    self.serverToJoin = PeerServer:new(server, self.context, function (s)
                                                              o:handleHeartbeatTimeout(s)
                                                             end)

    self:inviteServerToJoinCluster();
    response.accepted = true;
    return response;
end

function RaftServer:inviteServerToJoinCluster()
    local request = {
        commitIndex = self.quickCommitIndex,
        destination = self.serverToJoin:getId(),
        source = self.id,
        term = self.state.term,
        messageType = RaftMessageType.JoinClusterRequest,
        lastLogIndex = self.logStore:getFirstAvailableIndex() - 1,
        logEntries = {LogEntry:new(self.state.term, self.config:toBytes(), LogValueType.Configuration)},
    }

    local o = self
    self.serverToJoin:SendRequest(request, function (response, error)
                                               o:handleExtendedResponse(response, error);
                                           end);
end

function RaftServer:getSnapshotSyncBlockSize()
    local blockSize = self.context.raftParameters.snapshotBlockSize;
    return (blockSize == 0 and DEFAULT_SNAPSHOT_SYNC_BLOCK_SIZE) or blockSize;
end

function RaftServer:createSyncSnapshotRequest(peer, lastLogIndex, term, commitIndex)
    -- synchronized(peer){
    local context = peer:getSnapshotSyncContext();
    local snapshot 
    if context then
        snapshot = context.snapshot;
    end
    local lastSnapshot = self.stateMachine:getLastSnapshot();
    if(snapshot == nil or (lastSnapshot ~= nil and lastSnapshot.lastLogIndex > snapshot.lastLogIndex)) then
        snapshot = lastSnapshot;

        if(snapshot == nil or lastLogIndex > snapshot.lastLogIndex) then
            self.logger.error("system is running into fatal errors, failed to find a snapshot for peer %d(snapshot nil: %s, snapshot doesn't contais lastLogIndex: %s)", peer.getId(), String.valueOf(snapshot == nil), String.valueOf(lastLogIndex > snapshot.lastLogIndex));
            self.stateMachine:exit(-1);
            return nil;
        end

        if(snapshot.size < 1) then
            self.logger.error("invalid snapshot, this usually means a bug from state machine implementation, stop the system to prevent further errors");
            self.stateMachine:exit(-1);
            return nil;
        end

        self.logger.info("trying to sync snapshot with last index %d to peer %d", snapshot.lastLogIndex, peer.getId());
        peer.setSnapshotInSync(snapshot);
    end

    local offset = peer.snapshotSyncContext.offset;
    local sizeLeft = snapshot.size - offset;
    local blockSize = self:getSnapshotSyncBlockSize();
    local expectedSize = (sizeLeft > blockSize and blockSize) or sizeLeft;
    local data = {}
    -- try{
    local sizeRead, error = self.stateMachine:readSnapshotData(snapshot, offset, data, expectedSize);
    if error then
        -- if there is i/o error, no reason to continue
        self.logger.error("failed to read snapshot data due to io error %s", error.toString());
        self.stateMachine:exit(-1);
        return nil;
    end
    if(sizeRead and sizeRead < expectedSize) then
        self.logger.error("only %d bytes could be read from snapshot while %d bytes are expected, should be something wrong" , sizeRead, data.length);
        self.stateMachine:exit(-1);
        return nil;
    end

    local syncRequest = SnapshotSyncRequest:new(snapshot, offset, data, (offset + expectedSize) >= snapshot.size);
    local requestMessage = {
        messageType = RaftMessageType.InstallSnapshotRequest,
        source = self.id,
        destination = peer:getId(),
        lastLogIndex = snapshot.lastLogIndex,
        lastLogTerm = snapshot.lastLogTerm,
        logEntries = {LogEntry:new(term, syncRequest:toBytes(), LogValueType.SnapshotSyncRequest)},
        commitIndex = commitIndex,
        term = term,
    }
    return requestMessage;
end


function RaftServer:termForLastLog(logIndex)
    if(logIndex == 0) then
        return 0;
    end

    if(logIndex >= self.logStore:getStartIndex()) then
        return self.logStore:getLogEntryAt(logIndex).term;
    end
    local lastSnapshot = self.stateMachine:getLastSnapshot();
    if(lastSnapshot == nil or logIndex ~= lastSnapshot.lastLogIndex) then
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


        -- hitorical reason for logEntry to be nil
        if logEntry == nil then
            -- do nothing
            server.logger.error("committed an empty LogEntry!!")
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
        server:snapshotAndCompact(currentCommitIndex);
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