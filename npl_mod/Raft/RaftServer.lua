--[[
Title:
Author: liuluheng
Date: 2017.03.25
Desc:


------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/RaftServer.lua");
local RaftServer = commonlib.gettable("Raft.RaftServer");
------------------------------------------------------------
]]
--
NPL.load("(gl)script/ide/commonlib.lua")
NPL.load("(gl)npl_mod/Raft/RaftMessageSender.lua")
local RaftMessageSender = commonlib.gettable("Raft.RaftMessageSender")
NPL.load("(gl)npl_mod/Raft/ClusterServer.lua")
local ClusterServer = commonlib.gettable("Raft.ClusterServer")
NPL.load("(gl)npl_mod/Raft/ServerState.lua")
local ServerState = commonlib.gettable("Raft.ServerState")
NPL.load("(gl)npl_mod/Raft/LogEntry.lua")
local LogEntry = commonlib.gettable("Raft.LogEntry")
local LogValueType = NPL.load("(gl)npl_mod/Raft/LogValueType.lua")
local ServerRole = NPL.load("(gl)npl_mod/Raft/ServerRole.lua")
NPL.load("(gl)npl_mod/Raft/PeerServer.lua")
local PeerServer = commonlib.gettable("Raft.PeerServer")
NPL.load("(gl)script/ide/timer.lua")
local RaftMessageType = NPL.load("(gl)npl_mod/Raft/RaftMessageType.lua")
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua")
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)npl_mod/Raft/ClusterConfiguration.lua")
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration")
NPL.load("(gl)npl_mod/Raft/Snapshot.lua")
local Snapshot = commonlib.gettable("Raft.Snapshot")
NPL.load("(gl)npl_mod/Raft/SnapshotSyncRequest.lua")
local SnapshotSyncRequest = commonlib.gettable("Raft.SnapshotSyncRequest")
NPL.load("(gl)npl_mod/Raft/SnapshotSyncContext.lua")
local SnapshotSyncContext = commonlib.gettable("Raft.SnapshotSyncContext")
NPL.load("(gl)npl_mod/Raft/Rutils.lua")
local Rutils = commonlib.gettable("Raft.Rutils")

local RaftServer = commonlib.gettable("Raft.RaftServer")
local WALHandlerFile = "RPC/WALHandler.lua"

local DEFAULT_SNAPSHOT_SYNC_BLOCK_SIZE = 4 * 1024

local indexComparator = function(arg0, arg1)
  return arg0 > arg1
end

local real_commit  -- 'forward' declarations

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
    serverToJoin = nil,
    configChanging = false,
    catchingUp = false,
    steppingDown = 0
    -- end fields for extended messages
  }
  if not o.state then
    o.state = ServerState:new()
  end

  --[[
    * I found this implementation is also a victim of bug https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
    * As the implementation is based on Diego's thesis
    * Fix:
    * We should never load configurations that is not committed,
    *   this prevents an old server from replicating an obsoleted config to other servers
    * The prove is as below:
    * Assume S0 is the last committed server set for the old server A
    * |- EXITS Log l which has been committed but l !BELONGS TO A.logs =>  Vote(A) < Majority(S0)
    * In other words, we need to prove that A cannot be elected to leader if any logs/configs has been committed.
    * Case #1, There is no configuration change since S0, then it's obvious that Vote(A) < Majority(S0), see the core Algorithm
    * Case #2, There are one or more configuration changes since S0, then at the time of first configuration change was committed,
    *      there are at least Majority(S0 - 1) servers committed the configuration change
    *      Majority(S0 - 1) + Majority(S0) > S0 => Vote(A) < Majority(S0)
    * -|
    ]]
  --
  -- try to see if there is an uncommitted configuration change, since we cannot allow two configuration changes at a time
  for i = math.max(o.state.commitIndex + 1, o.logStore:getStartIndex()), o.logStore:getFirstAvailableIndex() - 1 do
    local logEntry = o.logStore:getLogEntryAt(i)
    if (logEntry.valueType == LogValueType.Configuration) then
      o.logger.info("detect a configuration change that is not committed yet at index %d", i)
      o.configChanging = true
      break
    end
  end

  o.electionTimer =
    commonlib.Timer:new(
    {
      callbackFunc = function(timer)
        o:handleElectionTimeout()
      end
    }
  ),
  setmetatable(o, self)

  for _, server in ipairs(o.config.servers) do
    if server.id ~= o.id then
      local peer =
        PeerServer:new(
        server,
        o.context,
        function(s)
          o:handleHeartbeatTimeout(s)
        end
      )
      o.peers[server.id] = peer
    end
  end
  o.quickCommitIndex = o.state.commitIndex

  -- election timer
  o:restartElectionTimer()

  -- register WAL Page handler
  NPL.this(
    function()
      o:handleWALRequest(msg)
    end,
    {filename = WALHandlerFile}
  )

  o.logger.info(format("Server %d started", o.id))
  return o
end

function RaftServer:__index(name)
  return rawget(self, name) or RaftServer[name]
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

  self.logger.debug(
    "Receive a %s message from %d with LastLogIndex=%d, LastLogTerm=%d, EntriesLength=%d, CommitIndex=%d and Term=%d",
    request.messageType.string,
    request.source or -1,
    request.lastLogIndex or -1,
    request.lastLogTerm or -1,
    entriesLength,
    request.commitIndex or 0,
    request.term or -1
  )

  local response
  if (request.messageType.int == RaftMessageType.AppendEntriesRequest.int) then
    response = self:handleAppendEntriesRequest(request)
  elseif (request.messageType.int == RaftMessageType.RequestVoteRequest.int) then
    response = self:handleVoteRequest(request)
  elseif (request.messageType.int == RaftMessageType.ClientRequest.int) then
    response = self:handleClientRequest(request)
  else
    -- extended requests
    response = self:handleExtendedMessages(request)
  end
  if (response ~= nil) then
    self.logger.debug(
      "Response back a %s message to %d with Accepted=%s, Term=%d, NextIndex=%d",
      response.messageType.string,
      response.destination,
      (response.accepted and "true") or "false",
      response.term,
      response.nextIndex or -1
    )
  else
    self.logger.error("why goes here?? response is nil")
    self.stateMachine:exit(-1)
  end
  return response
end

function RaftServer:handleAppendEntriesRequest(request)
  -- we allow the server to be continue after term updated to save a round message
  self:updateTerm(request.term)
  -- Reset stepping down value to prevent this server goes down when leader crashes after sending a LeaveClusterRequest
  if (self.steppingDown > 0) then
    self.steppingDown = 2
  end

  if (request.term == self.state.term) then
    if (self.role == ServerRole.Candidate) then
      self:becomeFollower()
    elseif (self.role == ServerRole.Leader) then
      self.logger.error(
        "Receive AppendEntriesRequest from another leader(%d) with same term, there must be a bug, server exits",
        request.source
      )
      self.stateMachine:exit(-1)
    else
      self:restartElectionTimer()
    end
  end
  local response = {
    messageType = RaftMessageType.AppendEntriesResponse,
    term = self.state.term,
    source = self.id,
    destination = request.source
  }

  -- After a snapshot the request.lastLogIndex may less than logStore:getStartingIndex() but equals to logStore:getStartingIndex() -1
  -- In this case, log is Okay if request.lastLogIndex == lastSnapshot.lastLogIndex and request.lastLogTerm == lastSnapshot.getLastTerm()
  local logOkay =
    request.lastLogIndex == 0 or
    (request.lastLogIndex < self.logStore:getFirstAvailableIndex() and
      request.lastLogTerm == self:termForLastLog(request.lastLogIndex))
  if (request.term < self.state.term or (not logOkay)) then
    response.accepted = false
    response.nextIndex = self.logStore:getFirstAvailableIndex()
    return response
  end
  -- The role is Follower and log is okay now
  if (request.logEntries ~= nil and #request.logEntries > 0) then
    -- write the logs to the store, first of all, check for overlap, and skip them
    local logEntries = request.logEntries
    local index = request.lastLogIndex + 1
    local logIndex = 1
    self.logger.trace("Init>%d entries, confirmedIndex:%d, entryIndex:%d", #logEntries, index, logIndex)
    while index < self.logStore:getFirstAvailableIndex() and logIndex < #logEntries + 1 and --do
      logEntries[logIndex].term == self.logStore:getLogEntryAt(index).term do
      logIndex = logIndex + 1
      index = index + 1
    end
    self.logger.trace("Olap>%d entries, confirmedIndex:%d, entryIndex:%d", #logEntries, index, logIndex)

    -- dealing with overwrites
    while (index < self.logStore:getFirstAvailableIndex() and logIndex < #logEntries + 1) do
      local oldEntry = self.logStore:getLogEntryAt(index)
      if (oldEntry.valueType == LogValueType.Application) then
        self.stateMachine:rollback(index, oldEntry.value)
      elseif (oldEntry.valueType == LogValueType.Configuration) then
        self.logger.info("revert a previous config change to config at %d", self.config.logIndex)
        self.configChanging = false
      end

      local logEntry = logEntries[logIndex]
      self.logStore:writeAt(index, logEntry)
      if (logEntry.valueType == LogValueType.Application) then
        self.stateMachine:preCommit(index, logEntry.value)
      elseif (logEntry.valueType == LogValueType.Configuration) then
        self.logger.info("received a configuration change at index %d from leader", index)
        self.configChanging = true
      end

      logIndex = logIndex + 1
      index = index + 1
    end
    self.logger.trace("Oite>%d entries, confirmedIndex:%d, entryIndex:%d", #logEntries, index, logIndex)
    -- append the new log entries
    while (logIndex < #logEntries + 1) do
      local logEntry = logEntries[logIndex]
      self.logger.trace("append entryIndex:" .. logIndex)
      logIndex = logIndex + 1
      local indexForEntry = self.logStore:append(logEntry)
      if (logEntry.valueType == LogValueType.Configuration) then
        self.logger.info("received a configuration change at index %d from leader", indexForEntry)
        self.configChanging = true
      elseif (logEntry.valueType == LogValueType.Application) then
        self.stateMachine:preCommit(indexForEntry, logEntry.value)
      end
    end
  end
  self.leader = request.source
  self:commit(request.commitIndex)
  response.accepted = true
  response.nextIndex = request.lastLogIndex + ((request.logEntries == nil and 0) or #request.logEntries) + 1
  return response
end

function RaftServer:handleVoteRequest(request)
  -- we allow the server to be continue after term updated to save a round message
  self:updateTerm(request.term)
  -- Reset stepping down value to prevent this server goes down when leader crashes after sending a LeaveClusterRequest
  if (self.steppingDown > 0) then
    self.steppingDown = 2
  end

  local response = {
    messageType = RaftMessageType.RequestVoteResponse,
    source = self.id,
    destination = request.source,
    term = self.state.term,
    nextIndex = 0
  }
  local logOkay =
    request.lastLogTerm > self.logStore:getLastLogEntry().term or
    (request.lastLogTerm == self.logStore:getLastLogEntry().term and
      self.logStore:getFirstAvailableIndex() - 1 <= request.lastLogIndex)
  local grant =
    request.term == self.state.term and logOkay and (self.state.votedFor == request.source or self.state.votedFor == -1)
  response.accepted = grant
  if (grant) then
    self.state.votedFor = request.source
    self.context.serverStateManager:persistState(self.state)
  end
  return response
end

function RaftServer:handleWALRequest(request)
  local term = self.state.term
  local logEntry = LogEntry:new(term, request)
  local logIndex = self.logStore:append(logEntry)
  self:requestAllAppendEntries()
end

function RaftServer:handleClientRequest(request)
  local response = {
    messageType = RaftMessageType.AppendEntriesResponse,
    source = self.id,
    destination = self.leader, -- use destination to indicate the leadId
    term = self.state.term
  }

  if self.role ~= ServerRole.Leader then
    response.accepted = false
    return response
  end

  local term = self.state.term

  self.logger.info(
    "ClientRequest start with logIndex %d, %d entries",
    self.logStore:getFirstAvailableIndex(),
    #request.logEntries
  )
  -- the leader executes the SQL, but the followers just append to WAL
  if request.logEntries and #request.logEntries > 0 then
    for _, v in ipairs(request.logEntries) do
      local logEntry = LogEntry:new(term, v.value)
      local logIndex = self.logStore:append(logEntry)
      self.stateMachine:preCommit(logIndex, v.value)
    end
  end

  self:requestAllAppendEntries()
  response.accepted = true
  -- nextIndex is not used in RaftClient for now
  response.nextIndex = self.logStore:getFirstAvailableIndex()
  return response
end

function RaftServer:handleElectionTimeout()
  if (self.steppingDown > 0) then
    self.steppingDown = self.steppingDown - 1
    if (self.steppingDown == 0) then
      self.logger.info("no hearing further news from leader, remove this server from config and step down")
      local server = self.config:getServer(self.id)
      if (server ~= nil) then
        self.config.servers[server.id] = nil
        self.context.serverStateManager:saveClusterConfiguration(self.config)
      end

      self.stateMachine:exit(0)
      return
    end

    self.logger.info("stepping down (cycles left: %d), skip this election timeout event", self.steppingDown)
    self:restartElectionTimer()
    return
  end

  if (self.catchingUp) then
    -- this is a new server for the cluster, will not send out vote request until the config that includes this server is committed
    self.logger.info("election timeout while joining the cluster, ignore it.")
    self:restartElectionTimer()
    return
  end

  if (self.role == ServerRole.Leader) then
    self.logger.error(
      "A leader should never encounter election timeout, illegal application state, stop the application"
    )
    self.stateMachine:exit(-1)
    return
  end

  self.logger.debug("Election timeout, change to Candidate")
  self.state:increaseTerm()
  self.state.votedFor = -1
  self.role = ServerRole.Candidate
  self.votesGranted = 0
  self.votesResponded = 0
  self.electionCompleted = false
  self.context.serverStateManager:persistState(self.state)
  self:requestVote()

  -- restart the election timer if this is not yet a leader
  if (self.role ~= ServerRole.Leader) then
    self:restartElectionTimer()
  end
end

function RaftServer:requestVote()
  -- vote for self
  self.logger.info("requestVote started with term %d", self.state.term)
  self.state.votedFor = self.id
  self.context.serverStateManager:persistState(self.state)
  self.votesGranted = self.votesGranted + 1
  self.votesResponded = self.votesResponded + 1

  -- this is the only server?
  if (self.votesGranted > (Rutils.table_size(self.peers) + 1) / 2) then
    self.electionCompleted = true
    self:becomeLeader()
    return
  end

  for _, peer in pairs(self.peers) do
    local request = {
      messageType = RaftMessageType.RequestVoteRequest,
      destination = peer:getId(),
      source = self.id,
      lastLogIndex = self.logStore:getFirstAvailableIndex() - 1,
      lastLogTerm = self:termForLastLog(self.logStore:getFirstAvailableIndex() - 1),
      term = self.state.term
    }

    self.logger.debug(
      "send %s to server %d with term %d",
      RaftMessageType.RequestVoteRequest.string,
      peer:getId(),
      self.state.term
    )
    local o = self
    peer:SendRequest(
      request,
      function(response, error)
        o:handlePeerResponse(response, error)
      end
    )
  end
end

function RaftServer:requestAllAppendEntries()
  -- be careful with the table and sequence array !
  -- self.logger.trace("#self.peers:%d, peers table size:%d", #self.peers, Rutils.table_size(self.peers))
  if (Rutils.table_size(self.peers) == 0) then
    self:commit(self.logStore:getFirstAvailableIndex() - 1)
    return
  end

  for _, peer in pairs(self.peers) do
    self:requestAppendEntries(peer)
  end
end

function RaftServer:requestAppendEntries(peer)
  if (peer:makeBusy()) then
    local o = self
    peer:SendRequest(
      self:createAppendEntriesRequest(peer),
      function(response, error)
        o:handlePeerResponse(response, error)
      end
    )
    return true
  end

  self.logger.debug("Server %d is busy, skip the request", peer:getId())
  return false
end

function RaftServer:handlePeerResponse(response, error)
  if (error ~= nil) then
    self.logger.info("peer response error: %s", error.string)
    return
  end

  self.logger.debug(
    "Receive a %s message from peer %d with Result=%s, Term=%d, NextIndex=%d",
    response.messageType.string,
    response.source,
    (response.accepted and "true") or "false",
    response.term,
    response.nextIndex
  )
  -- If term is updated no need to proceed
  if (self:updateTerm(response.term)) then
    return
  end

  -- Ignore the response that with lower term for safety
  if (response.term < self.state.term) then
    self.logger.info(
      "Received a peer response from %d that with lower term value %d v.s. %d",
      response.source,
      response.term,
      self.state.term
    )
    return
  end

  if (response.messageType.int == RaftMessageType.RequestVoteResponse.int) then
    self:handleVotingResponse(response)
  elseif (response.messageType.int == RaftMessageType.AppendEntriesResponse.int) then
    self:handleAppendEntriesResponse(response)
  elseif (response.messageType.int == RaftMessageType.InstallSnapshotResponse.int) then
    self:handleInstallSnapshotResponse(response)
  else
    self.logger.error("Received an unexpected message %s for response, system exits.", response.messageType.string)
    self.stateMachine:exit(-1)
  end
end

function RaftServer:handleAppendEntriesResponse(response)
  local peer = self.peers[response.source]
  if (peer == nil) then
    self.logger.info("the response is from an unkown peer %d", response.source)
    return
  end

  -- If there are pending logs to be synced or commit index need to be advanced, continue to send appendEntries to this peer
  local needToCatchup = true
  if (response.accepted) then
    peer.nextLogIndex = response.nextIndex
    peer.matchedIndex = response.nextIndex - 1

    -- try to commit with this response
    local matchedIndexes = {}
    matchedIndexes[#matchedIndexes + 1] = self.logStore:getFirstAvailableIndex() - 1
    for _, p in pairs(self.peers) do
      matchedIndexes[#matchedIndexes + 1] = p.matchedIndex
    end

    self.logger.trace("all matchedIndexes:%s", util.table_tostring(matchedIndexes))
    table.sort(matchedIndexes, indexComparator)
    self.logger.trace("sorted matchedIndexes:%s", util.table_tostring(matchedIndexes))
    -- majority is a float without math.ceil
    -- lua index start form 1
    local majority = math.ceil((Rutils.table_size(self.peers) + 1) / 2)
    self.logger.trace("total server:%d, major:%d", Rutils.table_size(self.peers) + 1, majority)
    self:commit(matchedIndexes[majority])
    needToCatchup = peer:clearPendingCommit() or response.nextIndex < self.logStore:getFirstAvailableIndex()
  else
    -- Improvement: if peer's real log length is less than was assumed, reset to that length directly
    if (response.nextIndex > 0 and peer.nextLogIndex > response.nextIndex) then
      peer.nextLogIndex = response.nextIndex
    else
      peer.nextLogIndex = peer.nextLogIndex - 1
    end

    -- TODO: step down when leader recv higher term 
    -- see https://github.com/brpc/braft/blob/master/docs/cn/raft_protocol.md#symmetric-network-partitioning
    -- and https://github.com/brpc/braft/blob/f7b8591cca13bf8b003b87091a9495daf69c380b/src/braft/replicator.cpp#L326
  end

  -- This may not be a leader anymore, such as the response was sent out long time ago
  -- and the role was updated by UpdateTerm call
  -- Try to match up the logs for this peer
  if (self.role == ServerRole.Leader and needToCatchup) then
    self.logger.debug("Server %d needToCatchup", peer:getId())
    self:requestAppendEntries(peer)
  end
end

function RaftServer:handleInstallSnapshotResponse(response)
  local peer = self.peers[response.source]
  if (peer == nil) then
    self.logger.info("the response is from an unkonw peer %d", response.source)
    return
  end

  -- If there are pending logs to be synced or commit index need to be advanced, continue to send appendEntries to this peer
  local needToCatchup = true
  if (response.accepted) then
    local context = peer.snapshotSyncContext
    if (context == nil) then
      self.logger.info("no snapshot sync context for this peer, drop the response")
      needToCatchup = false
    else
      local currentSyncCollectionName = context.snapshot.collectionsNameSize[context.currentCollectionIndex].name
      if
        (context.currentCollectionIndex >= #context.snapshot.collectionsNameSize and
          response.nextIndex >= context.snapshot.size)
       then
        self.logger.debug("snapshot sync is done")
        peer.nextLogIndex = context.snapshot.lastLogIndex + 1
        peer.matchedIndex = context.snapshot.lastLogIndex
        peer.snapshotSyncContext = nil
        needToCatchup = peer:clearPendingCommit() or response.nextIndex < self.logStore:getFirstAvailableIndex()
      elseif (response.nextIndex >= context.snapshot.size) then
        self.logger.debug(
          "continue to sync snapshot for %s at offset %d",
          currentSyncCollectionName,
          response.nextIndex
        )
        context.currentCollectionIndex = context.currentCollectionIndex + 1
        local currentSyncCollectionSize = context.snapshot.collectionsNameSize[context.currentCollectionIndex].size
        context.snapshot.size = currentSyncCollectionSize
        context.offset = 0
      else
        self.logger.debug(
          "continue to sync snapshot for %s at offset %d",
          currentSyncCollectionName,
          response.nextIndex
        )
        context.offset = response.nextIndex
      end
    end
  else
    -- Improvement: if peer's real log length is bigger than was assumed, reset to that length directly
    if (response.nextIndex > 0 and peer.nextLogIndex < response.nextIndex) then
      peer.nextLogIndex = response.nextIndex
      peer.matchedIndex = response.nextIndex - 1
      peer.snapshotSyncContext = nil
    end
    self.logger.info(
      "peer (%d) declines (%d) to install the snapshot (%d), may retry",
      peer:getId(),
      peer.nextLogIndex,
      self.logStore:getStartIndex()
    )
  end

  -- This may not be a leader anymore, such as the response was sent out long time ago
  -- and the role was updated by UpdateTerm call
  -- Try to match up the logs for this peer
  if (self.role == ServerRole.Leader and needToCatchup) then
    self:requestAppendEntries(peer)
  end
end

function RaftServer:handleVotingResponse(response)
  self.votesResponded = self.votesResponded + 1
  if (self.electionCompleted) then
    self.logger.info("Election completed, will ignore the voting result from this server")
    return
  end

  if (response.accepted) then
    self.votesGranted = self.votesGranted + 1
  end

  if (self.votesResponded >= Rutils.table_size(self.peers) + 1) then
    self.electionCompleted = true
  end

  -- got a majority set of granted votes
  if (self.votesGranted > (Rutils.table_size(self.peers) + 1) / 2) then
    self.logger.info("Server is elected as leader for term %d", self.state.term)
    self.electionCompleted = true
    self:becomeLeader()
  end
end

function RaftServer:handleHeartbeatTimeout(peer)
  self.logger.debug("Heartbeat timeout for %d", peer:getId())
  if (self.role == ServerRole.Leader) then
    self:requestAppendEntries(peer)
    if (peer.heartbeatEnabled) then
      -- Schedule another heartbeat if heartbeat is still enabled
      peer.heartbeatTimer:Change(peer.currentHeartbeatInterval, nil)
    else
      self.logger.debug("heartbeat is disabled for peer %d", peer:getId())
    end
  else
    self.logger.info("Receive a heartbeat event for %d while no longer as a leader", peer:getId())
  end
end

function RaftServer:restartElectionTimer()
  -- don't start the election timer while this server is still catching up the logs
  if (self.catchingUp) then
    return
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
    self.logger.error("Election Timer is never started but is requested to stop, protential a bug")
  end

  self.electionTimer:Change()
  assert(not self.electionTimer:IsEnabled(), "stopElectionTimer error")
end

function RaftServer:becomeLeader()
  self:stopElectionTimer()
  self.role = ServerRole.Leader
  self.leader = self.id
  self.serverToJoin = nil
  for _, server in pairs(self.peers) do
    server.nextLogIndex = self.logStore:getFirstAvailableIndex()
    server.snapshotSyncContext = nil
    server:setFree()
    self:enableHeartbeatForPeer(server)
  end

  -- if current config is not committed, try to commit it
  if (self.config.logIndex == 0) then
    self.config.logIndex = self.logStore:getFirstAvailableIndex()
    self.logStore:append(LogEntry:new(self.state.term, self.config:toBytes(), LogValueType.Configuration))
    self.logger.info("add initial configuration to log store")
    self.configChanging = true
  end

  self:requestAllAppendEntries()
end

function RaftServer:enableHeartbeatForPeer(peer)
  peer.heartbeatEnabled = true
  peer:resumeHeartbeatingSpeed()
  peer.heartbeatTimer:Change(peer.currentHeartbeatInterval, nil)
  assert(peer.heartbeatTimer:IsEnabled(), "start heartbeatTimer error")
end

function RaftServer:becomeFollower()
  -- stop heartbeat for all peers
  for _, server in pairs(self.peers) do
    if server.heartbeatTimer:IsEnabled() then
      server.heartbeatTimer:Change()
      assert(not server.heartbeatTimer:IsEnabled(), "stop heartbeatTimer error")
    end
    server.heartbeatEnabled = false
  end
  self.serverToJoin = nil
  self.role = ServerRole.Follower
  self:restartElectionTimer()
end

function RaftServer:updateTerm(term)
  if (term > self.state.term) then
    self.state.term = term
    self.state.votedFor = -1
    self.electionCompleted = false
    self.votesGranted = 0
    self.votesResponded = 0
    self.context.serverStateManager:persistState(self.state)
    self:becomeFollower()
    return true
  end

  return false
end

function RaftServer:commit(targetIndex)
  self.logger.trace("commit index:%d, quickCommitIndex:%d", targetIndex, self.quickCommitIndex)
  if (targetIndex > self.quickCommitIndex) then
    self.quickCommitIndex = targetIndex

    -- if this is a leader notify peers to commit as well
    -- for peers that are free, send the request, otherwise, set pending commit flag for that peer
    if (self.role == ServerRole.Leader) then
      for _, peer in pairs(self.peers) do
        if (not self:requestAppendEntries(peer)) then
          peer:setPendingCommit()
        end
      end
    end
  end

  if
    (self.logStore:getFirstAvailableIndex() - 1 > self.state.commitIndex and
      self.quickCommitIndex > self.state.commitIndex)
   then
    real_commit(self)
  end
end

function RaftServer:snapshotAndCompact(indexCommitted)
  local snapshotInAction = false
  -- see if we need to do snapshots
  self.logger.trace(
    "snapshotAndCompact>snapshotDistance:%d, indexCommitted:%d, logStore:getStartIndex:%d, snapshotInProgress:%d",
    self.context.raftParameters.snapshotDistance,
    indexCommitted,
    self.logStore:getStartIndex(),
    self.snapshotInProgress
  )
  if
    (self.context.raftParameters.snapshotDistance > 0 and
      ((indexCommitted - self.logStore:getStartIndex()) > self.context.raftParameters.snapshotDistance) and
      self.snapshotInProgress == 0)
   then
    self.snapshotInProgress = 1
    snapshotInAction = true
    local currentSnapshot = self.stateMachine:getLastSnapshot()
    if
      (currentSnapshot ~= nil and
        indexCommitted - currentSnapshot.lastLogIndex < self.context.raftParameters.snapshotDistance)
     then
      self.logger.info(
        "a very recent snapshot is available at index %d, will skip this one",
        currentSnapshot.lastLogIndex
      )
      self.snapshotInProgress = 0
      snapshotInAction = false
    else
      self.logger.info("creating a snapshot for index %d", indexCommitted)

      -- get the latest configuration info
      local config = self.config
      while (config.logIndex > indexCommitted and config.lastLogIndex >= self.logStore:getStartIndex()) do
        local configLog = self.logStore:getLogEntryAt(config.lastLogIndex)
        if not configLog then
          self.logger.error("cannot find config at %d", config.lastLogIndex)
          self.stateMachine:exit(-1)
        end
        config = ClusterConfiguration:fromBytes(configLog.value)
      end

      if
        (config.logIndex > indexCommitted and config.lastLogIndex > 0 and
          config.lastLogIndex < self.logStore:getStartIndex())
       then
        local lastSnapshot = self.stateMachine:getLastSnapshot()
        if (lastSnapshot == nil) then
          self.logger.error(
            "No snapshot could be found while no configuration cannot be found in current committed logs, this is a system error, exiting"
          )
          self.stateMachine:exit(-1)
          return
        end

        config = lastSnapshot.lastConfig
      elseif (config.logIndex > indexCommitted and config.lastLogIndex == 0) then
        self.logger.error("BUG!!! stop the system, there must be a configuration at index one")
        self.stateMachine:exit(-1)
      end

      local indexToCompact = indexCommitted - 1
      local logTermToCompact = self.logStore:getLogEntryAt(indexToCompact).term
      local snapshot = Snapshot:new(indexToCompact, logTermToCompact, config)

      local o = self
      local result, err = self.stateMachine:createSnapshot(snapshot)
      if (err ~= nil) then
        o.logger.error("failed to create a snapshot due to %s", err)
      end

      if (not result) then
        o.logger.info("the state machine rejects to create the snapshot")
      end

      if not err and result then
        o.logger.debug("snapshot created, compact the log store")

        o.logStore:compact(snapshot.lastLogIndex)
      -- o.logger.error("failed to compact the log store, no worries, the system still in a good shape", ex);
      end

      self.snapshotInProgress = 0
      snapshotInAction = false
    end
  end
  -- error handle
  -- self.logger.error("failed to compact logs at index %d, due to errors %s", indexCommitted, error);
  -- if(snapshotInAction) then
  --     self.snapshotInProgress = 0;
  -- end
end

function RaftServer:createAppendEntriesRequest(peer)
  local startingIndex = self.logStore:getStartIndex()
  local currentNextIndex = self.logStore:getFirstAvailableIndex()
  local commitIndex = self.quickCommitIndex
  local term = self.state.term

  if (peer.nextLogIndex == 0) then
    peer.nextLogIndex = currentNextIndex
  end

  local lastLogIndex = peer.nextLogIndex - 1
  if (lastLogIndex >= currentNextIndex) then
    self.logger.error("Peer's lastLogIndex is too large %d v.s. %d, server exits", lastLogIndex, currentNextIndex)
    self.stateMachine:exit(-1)
  end
  -- for syncing the snapshots, if the lastLogIndex == lastSnapshot.getLastLogIndex, we could get the term from the snapshot
  if (lastLogIndex > 0 and lastLogIndex < startingIndex - 1) then
    -- this may also happens when the peer's lastLogIndex is stale due to the response err
    self.logger.trace("sync snapshot %d to peer %d, %d", startingIndex, peer:getId(), lastLogIndex)
    return self:createSyncSnapshotRequest(peer, lastLogIndex, term, commitIndex)
  end
  local lastLogTerm = self:termForLastLog(lastLogIndex)
  local endIndex = math.min(currentNextIndex, lastLogIndex + 1 + self.context.raftParameters.maximumAppendingSize)
  local logEntries
  if not ((lastLogIndex + 1) >= endIndex) then
    logEntries = self.logStore:getLogEntries(lastLogIndex + 1, endIndex)
  end
  self.logger.trace(
    "endIndex:%d, currentNextIndex:%d, max endIndex%d",
    endIndex,
    currentNextIndex,
    lastLogIndex + 1 + self.context.raftParameters.maximumAppendingSize
  )
  self.logger.debug(
    "An AppendEntries Request for %d with LastLogIndex=%d, LastLogTerm=%d, EntriesLength=%d, CommitIndex=%d and Term=%d",
    peer:getId(),
    lastLogIndex,
    lastLogTerm,
    (logEntries == nil and 0) or #logEntries,
    commitIndex,
    term
  )
  local requestMessage = {
    messageType = RaftMessageType.AppendEntriesRequest,
    source = self.id,
    destination = peer:getId(),
    lastLogIndex = lastLogIndex,
    lastLogTerm = lastLogTerm,
    logEntries = logEntries,
    commitIndex = commitIndex,
    term = term
  }
  return requestMessage
end

-- private
function RaftServer:reconfigure(newConfig)
  self.logger.debug(
    "system is reconfigured to have %d servers, last config index: %d, this config index: %d",
    Rutils.table_size(newConfig.servers),
    newConfig.lastLogIndex,
    newConfig.logIndex
  )

  local serversRemoved = {}
  local serversAdded = {}

  for _, server in ipairs(newConfig.servers) do
    if not self.peers[server.id] and server.id ~= self.id then
      -- new add server
      serversAdded[server] = true
    end
  end

  for id, _ in pairs(self.peers) do
    if newConfig:getServer(id) == nil then
      -- the server has been removed
      serversRemoved[id] = true
    end
  end

  if newConfig:getServer(self.id) == nil then
    serversRemoved[self.id] = true
  end

  for server, _ in pairs(serversAdded) do
    if (server.id ~= self.id) then
      local o = self
      local peer =
        PeerServer:new(
        server,
        self.context,
        function(s)
          o:handleHeartbeatTimeout(s)
        end
      )
      peer.nextLogIndex = self.logStore:getFirstAvailableIndex()
      self.peers[server.id] = peer
      self.logger.info("server %d is added to cluster", peer:getId())
      -- add server to NPL
      Rutils.addServerToNPLRuntime(server)
      if (self.role == ServerRole.Leader) then
        self.logger.info("enable heartbeating for server %d", peer:getId())
        self:enableHeartbeatForPeer(peer)
        if (self.serverToJoin ~= nil and self.serverToJoin:getId() == peer:getId()) then
          peer.nextLogIndex = self.serverToJoin.nextLogIndex
          self.serverToJoin = nil
        end
      end
    end
  end

  for id, _ in pairs(serversRemoved) do
    if (id == self.id and not self.catchingUp) then
      -- this server is removed from cluster
      self.context.serverStateManager:saveClusterConfiguration(newConfig)
      self.logger.info("server has been removed from cluster, step down")
      self.stateMachine:exit(0)
      return
    end

    local peer = self.peers[id]
    if (peer == nil) then
      self.logger.info("peer %d cannot be found in current peer list", id)
    else
      peer.heartbeatTimer:Change()
      peer.heartbeatEnabled = false
      self.peers[id] = nil
      self.logger.info("server %d is removed from cluster", id)
    end
  end

  self.config = newConfig
end

function RaftServer:handleExtendedMessages(request)
  if (request.messageType.int == RaftMessageType.AddServerRequest.int) then
    return self:handleAddServerRequest(request)
  elseif (request.messageType.int == RaftMessageType.RemoveServerRequest.int) then
    return self:handleRemoveServerRequest(request)
  elseif (request.messageType.int == RaftMessageType.SyncLogRequest.int) then
    return self:handleLogSyncRequest(request)
  elseif (request.messageType.int == RaftMessageType.JoinClusterRequest.int) then
    return self:handleJoinClusterRequest(request)
  elseif (request.messageType.int == RaftMessageType.LeaveClusterRequest.int) then
    return self:handleLeaveClusterRequest(request)
  elseif (request.messageType.int == RaftMessageType.InstallSnapshotRequest.int) then
    return self:handleInstallSnapshotRequest(request)
  else
    self.logger.error("receive an unknown request %s, for safety, step down.", request.messageType.string)
    self.stateMachine:exit(-1)
  end
end

function RaftServer:handleInstallSnapshotRequest(request)
  -- we allow the server to be continue after term updated to save a round message
  self:updateTerm(request.term)

  -- Reset stepping down value to prevent this server goes down when leader crashes after sending a LeaveClusterRequest
  if (self.steppingDown > 0) then
    self.steppingDown = 2
  end

  if (request.term == self.state.term and not self.catchingUp) then
    if (self.role == ServerRole.Candidate) then
      self:becomeFollower()
    elseif (self.role == ServerRole.Leader) then
      self.logger.error(
        "Receive InstallSnapshotRequest from another leader(%d) with same term, there must be a bug, server exits",
        request.source
      )
      self.stateMachine:exit(-1)
    else
      self:restartElectionTimer()
    end
  end

  local response = {
    messageType = RaftMessageType.InstallSnapshotResponse,
    term = self.state.term,
    source = self.id,
    destination = request.source
  }

  if (not self.catchingUp and request.term < self.state.term) then
    self.logger.info("received an install snapshot request which has lower term than this server, decline the request")
    response.accepted = false
    response.nextIndex = 0
    return response
  end

  local logEntries = request.logEntries
  if (logEntries == nil or #logEntries ~= 1 or logEntries[1].valueType ~= LogValueType.SnapshotSyncRequest) then
    self.logger.warn("Receive an invalid InstallSnapshotRequest due to bad log entries or bad log entry value")
    response.nextIndex = 0
    response.accepted = false
    return response
  end

  local snapshotSyncRequest = SnapshotSyncRequest:fromBytes(logEntries[1].value)

  -- We don't want to apply a snapshot that is older than we have, this may not happen, but just in case
  -- if (snapshotSyncRequest.snapshot.lastLogIndex <= self.quickCommitIndex) then
  if (snapshotSyncRequest.snapshot.lastLogIndex <= self.logStore:getFirstAvailableIndex()) then
    self.logger.error(
      "Received a snapshot (%d) which is older than this (%d) server (%d, %d)",
      snapshotSyncRequest.snapshot.lastLogIndex,
      self.id,
      self.quickCommitIndex,
      self.logStore:getFirstAvailableIndex()
    )
    response.nextIndex = self.logStore:getFirstAvailableIndex()
    response.accepted = false
    return response
  end

  response.accepted = self:handleSnapshotSyncRequest(snapshotSyncRequest)
  response.nextIndex = snapshotSyncRequest.offset + #snapshotSyncRequest.data -- the next index will be ignored if "accept" is false
  return response
end

function RaftServer:handleSnapshotSyncRequest(snapshotSyncRequest)
  self.stateMachine:saveSnapshotData(
    snapshotSyncRequest.snapshot,
    snapshotSyncRequest.currentCollectionName,
    snapshotSyncRequest.offset,
    snapshotSyncRequest.data
  )
  if (snapshotSyncRequest.done) then
    -- Only follower will run this piece of code, but let's check it again
    if (self.role ~= ServerRole.Follower) then
      self.logger.error("bad server role for applying a snapshot, exit for debugging")
      self.stateMachine:exit(-1)
    end

    self.logger.debug("sucessfully receive a snapshot from leader")
    if (self.logStore:compact(snapshotSyncRequest.snapshot.lastLogIndex)) then
      -- The state machine will not be able to commit anything before the snapshot is applied, so make this synchronously
      -- with election timer stopped as usually applying a snapshot may take a very long time
      self:stopElectionTimer()
      self.logger.info("successfully compact the log store, will now ask the statemachine to apply the snapshot")
      if (not self.stateMachine:applySnapshot(snapshotSyncRequest.snapshot)) then
        self.logger.error(
          "failed to apply the snapshot after log compacted, to ensure the safety, will shutdown the system"
        )
        self.stateMachine:exit(-1)
        return false --should never be reached
      end

      self:reconfigure(snapshotSyncRequest.snapshot.lastConfig)
      self.context.serverStateManager:saveClusterConfiguration(self.config)
      self.state.commitIndex = snapshotSyncRequest.snapshot.lastLogIndex
      self.quickCommitIndex = snapshotSyncRequest.snapshot.lastLogIndex
      self.context.serverStateManager:persistState(self.state)
      self.logger.info("snapshot is successfully applied")
      self:restartElectionTimer()
    else
      self.logger.error("failed to compact the log store after a snapshot is received, will ask the leader to retry")
      return false
    end
  end

  return true
end

function RaftServer:handleExtendedResponse(response, error)
  if (error ~= nil) then
    self:handleExtendedResponseError(error)
    return
  end

  self.logger.debug(
    "Receive an extended %s message from peer %d with Result=%s, Term=%d, NextIndex=%d",
    response.messageType.string,
    response.source,
    (response.accepted and "true") or "false",
    response.term,
    response.nextIndex
  )
  if (response.messageType.int == RaftMessageType.SyncLogResponse.int) then
    if (self.serverToJoin ~= nil) then
      -- we are reusing heartbeat interval value to indicate when to stop retry
      self.serverToJoin:resumeHeartbeatingSpeed()
      self.serverToJoin.nextLogIndex = response.nextIndex
      self.serverToJoin.matchedIndex = response.nextIndex - 1
      self:syncLogsToNewComingServer(response.nextIndex)
    end
  elseif (response.messageType.int == RaftMessageType.JoinClusterResponse.int) then
    if (self.serverToJoin ~= nil) then
      if (response.accepted) then
        self.logger.debug("new server confirms it will join, start syncing logs to it")
        self:syncLogsToNewComingServer(1)
      else
        self.logger.debug("new server cannot accept the invitation, give up")
      end
    else
      self.logger.debug("no server to join, drop the message")
    end
  elseif (response.messageType.int == RaftMessageType.LeaveClusterResponse.int) then
    if (not response.accepted) then
      self.logger.info("peer doesn't accept to stepping down, stop proceeding")
      return
    end

    self.logger.debug("peer accepted to stepping down, removing this server from cluster")
    self:removeServerFromCluster(response.source)
  elseif (response.messageType.int == RaftMessageType.InstallSnapshotResponse.int) then
    if (self.serverToJoin == nil) then
      self.logger.info("no server to join, the response must be very old.")
      return
    end

    if (not response.accepted) then
      self.logger.info("peer doesn't accept the snapshot installation request")
      return
    end

    local context = self.serverToJoin.snapshotSyncContext
    if (context == nil) then
      self.logger.error("Bug! SnapshotSyncContext must not be nil")
      self.stateMachine:exit(-1)
      return
    end

    local currentSyncCollectionName = context.snapshot.collectionsNameSize[context.currentCollectionIndex].name
    if
      (context.currentCollectionIndex >= #context.snapshot.collectionsNameSize and
        response.nextIndex >= context.snapshot.size)
     then
      -- snapshot is done
      self.logger.debug("snapshot has been copied and applied to new server, continue to sync logs after snapshot")
      self.serverToJoin.snapshotSyncContext = nil
      self.serverToJoin.nextLogIndex = context.snapshot.lastLogIndex + 1
      self.serverToJoin.matchedIndex = context.snapshot.lastLogIndex
    elseif (response.nextIndex >= context.snapshot.size) then
      self.logger.debug("continue to sync snapshot for %s at offset %d", currentSyncCollectionName, response.nextIndex)
      context.currentCollectionIndex = context.currentCollectionIndex + 1
      local currentSyncCollectionSize = context.snapshot.collectionsNameSize[context.currentCollectionIndex].size
      context.snapshot.size = currentSyncCollectionSize
      context.offset = 0
    else
      self.logger.debug("continue to sync snapshot for %s at offset %d", currentSyncCollectionName, response.nextIndex)
      context.offset = response.nextIndex
    end

    self:syncLogsToNewComingServer(self.serverToJoin.nextLogIndex)
  else
    -- No more response message types need to be handled
    self.logger.error(
      "received an unexpected response message type %s, for safety, stepping down",
      response.messageType.string
    )
    self.stateMachine:exit(-1)
  end
end

function RaftServer:handleExtendedResponseError(error)
  self.logger.info("receive an error response from peer server, %s", error.string)

  if (error ~= nil) then
    self.logger.debug("it's a rpc error, see if we need to retry")
    local request = error.request
    if
      (request.messageType.int == RaftMessageType.SyncLogRequest.int or
        request.messageType.int == RaftMessageType.JoinClusterRequest.int or
        request.messageType.int == RaftMessageType.LeaveClusterRequest.int)
     then
      local server =
        (request.messageType.int == RaftMessageType.LeaveClusterRequest.int) and self.peers[request.destination] or
        self.serverToJoin
      if (server ~= nil) then
        if (server.currentHeartbeatInterval >= self.context.raftParameters:getMaxHeartbeatInterval()) then
          if (request.messageType.int == RaftMessageType.LeaveClusterRequest.int) then
            self.logger.info(
              "rpc failed again for the removing server (%d), will remove this server directly",
              server:getId()
            )

            --[[
                        * In case of there are only two servers in the cluster, it safe to remove the server directly from peers
                        * as at most one config change could happen at a time
                        *  prove:
                        *      assume there could be two config changes at a time
                        *      this means there must be a leader after previous leader offline, which is impossible
                        *      (no leader could be elected after one server goes offline in case of only two servers in a cluster)
                        * so the bug https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
                        * does not apply to cluster which only has two members
                        ]]
            --
            if (Rutils.table_size(self.peers) == 1) then
              local peer = self.peers[server:getId()]
              if (peer == nil) then
                self.logger.info("peer %d cannot be found in current peer list", id)
              else
                peer.heartbeatTimer:Change()
                peer.heartbeatEnabled = false
                self.peers[server:getId()] = nil
                self.logger.info("server %d is removed from cluster", server:getId())
              end
            end

            self:removeServerFromCluster(server:getId())
          else
            self.logger.info(
              "rpc failed again for the new coming server (%d), will stop retry for this server",
              server:getId()
            )
            self.configChanging = false
            self.serverToJoin = nil
          end
        else
          -- reuse the heartbeat interval value to indicate when to stop retrying, as rpc backoff is the same
          self.logger.debug("retry the request")
          server:slowDownHeartbeating()
          local this = self

          local timer =
            commonlib.Timer:new(
            {
              callbackFunc = function(timer)
                this.logger.debug("retrying the request %s", request.messageType.string)

                server:SendRequest(
                  request,
                  function(furtherResponse, furtherError)
                    this:handleExtendedResponse(furtherResponse, furtherError)
                  end
                )
                return nil
              end
            }
          )
          timer:Change(server.currentHeartbeatInterval, nil)
        end
      end
    end
  end
end

function RaftServer:handleRemoveServerRequest(request)
  local logEntries = request.logEntries
  local response = {
    source = self.id,
    destination = self.leader,
    term = self.state.term,
    messageType = RaftMessageType.RemoveServerResponse,
    nextIndex = self.logStore:getFirstAvailableIndex(),
    accepted = false
  }

  if (#logEntries ~= 1 or logEntries[1].valueType ~= LogValueType.ClusterServer) then
    self.logger.info("bad add server request as we are expecting one log entry with value type of ClusterServer")
    return response
  end

  if (self.role ~= ServerRole.Leader) then
    self.logger.info("this is not a leader, cannot handle RemoveServerRequest")
    return response
  end

  if (self.configChanging) then
    -- the previous config has not committed yet
    self.logger.info("previous config has not committed yet")
    return response
  end

  local serverId = tonumber(logEntries[1].value)
  if (serverId == self.id) then
    self.logger.info("cannot request to remove leader")
    return response
  end

  local peer = self.peers[serverId]
  if (peer == nil) then
    -- self.logger.trace("serverId type:%s, %s", type(serverId), serverId)
    self.logger.info("server %d does not exist", tonumber(serverId))
    return response
  end

  local leaveClusterRequest = {
    commitIndex = self.quickCommitIndex,
    destination = peer:getId(),
    lastLogIndex = self.logStore:getFirstAvailableIndex() - 1,
    lastLogTerm = 0,
    term = self.state.term,
    messageType = RaftMessageType.LeaveClusterRequest,
    source = self.id
  }

  local o = self
  peer:SendRequest(
    leaveClusterRequest,
    function(response, error)
      o:handleExtendedResponse(response, error)
    end
  )

  response.accepted = true
  return response
end

function RaftServer:handleAddServerRequest(request)
  local logEntries = request.logEntries
  local response = {
    source = self.id,
    destination = self.leader,
    term = self.state.term,
    messageType = RaftMessageType.AddServerResponse,
    nextIndex = self.logStore:getFirstAvailableIndex(),
    accepted = false
  }

  if (#logEntries ~= 1 or logEntries[1].valueType ~= LogValueType.ClusterServer) then
    self.logger.info("bad add server request as we are expecting one log entry with value type of ClusterServer")
    return response
  end

  if (self.role ~= ServerRole.Leader) then
    self.logger.info("this is not a leader, cannot handle AddServerRequest")
    return response
  end

  local server = ClusterServer:new(logEntries[1].value)
  if (self.peers[server.Id] or self.id == server.id) then
    self.logger.warn("the server to be added has a duplicated id with existing server %d", server.id)
    return response
  end

  if (self.configChanging) then
    -- the previous config has not committed yet
    self.logger.info("previous config has not committed yet")
    return response
  end

  local o = self
  self.serverToJoin =
    PeerServer:new(
    server,
    self.context,
    function(s)
      o:handleHeartbeatTimeout(s)
    end
  )

  -- add server to NPL
  Rutils.addServerToNPLRuntime(server)

  self:inviteServerToJoinCluster()
  response.accepted = true
  return response
end

function RaftServer:handleLogSyncRequest(request)
  local logEntries = request.logEntries
  local response = {
    source = self.id,
    destination = request.source,
    term = self.state.term,
    messageType = RaftMessageType.SyncLogResponse,
    nextIndex = self.logStore:getFirstAvailableIndex(),
    accepted = false
  }

  if
    (logEntries == nil or #logEntries ~= 1 or logEntries[1].valueType ~= LogValueType.LogPack or
      logEntries[1].value == nil or
      #logEntries[1].value == 0)
   then
    self.logger.info("receive an invalid LogSyncRequest as the log entry value doesn't meet the requirements")
    return response
  end

  if (not self.catchingUp) then
    self.logger.debug("This server is ready for cluster, ignore the request")
    return response
  end

  self.logStore:applyLogPack(request.lastLogIndex + 1, logEntries[1].value)
  self:commit(self.logStore:getFirstAvailableIndex() - 1)
  response.nextIndex = self.logStore:getFirstAvailableIndex()
  response.accepted = true
  return response
end

function RaftServer:syncLogsToNewComingServer(startIndex)
  -- only sync committed logs
  local gap = self.quickCommitIndex - startIndex
  if (gap < self.context.raftParameters.logSyncStopGap) then
    self.logger.info(
      "LogSync is done for server %d with log gap %d, now put the server into cluster",
      self.serverToJoin:getId(),
      gap
    )
    local newConfig = ClusterConfiguration:new(self.config)
    newConfig.logIndex = self.logStore:getFirstAvailableIndex()
    newConfig.servers[#newConfig.servers + 1] = ClusterServer:new(self.serverToJoin.clusterConfig)
    local configEntry = LogEntry:new(self.state.term, newConfig:toBytes(), LogValueType.Configuration)
    self.logStore:append(configEntry)
    self.configChanging = true
    self:requestAllAppendEntries()
    return
  end

  local request = nil
  if (startIndex > 0 and startIndex < self.logStore:getStartIndex()) then
    request = self:createSyncSnapshotRequest(self.serverToJoin, startIndex, self.state.term, self.quickCommitIndex)
  else
    local sizeToSync = math.min(gap, self.context.raftParameters.logSyncBatchSize)
    local logPack = self.logStore:packLog(startIndex, sizeToSync)
    request = {
      commitIndex = self.quickCommitIndex,
      destination = self.serverToJoin:getId(),
      source = self.id,
      term = self.state.term,
      messageType = RaftMessageType.SyncLogRequest,
      lastLogIndex = startIndex - 1,
      logEntries = {LogEntry:new(self.state.term, logPack, LogValueType.LogPack)}
    }
  end

  local this = self
  self.serverToJoin:SendRequest(
    request,
    function(response, error)
      this:handleExtendedResponse(response, error)
    end
  )
end

function RaftServer:inviteServerToJoinCluster()
  local request = {
    commitIndex = self.quickCommitIndex,
    destination = self.serverToJoin:getId(),
    source = self.id,
    term = self.state.term,
    messageType = RaftMessageType.JoinClusterRequest,
    lastLogIndex = self.logStore:getFirstAvailableIndex() - 1,
    logEntries = {LogEntry:new(self.state.term, self.config:toBytes(), LogValueType.Configuration)}
  }

  local o = self
  self.serverToJoin:SendRequest(
    request,
    function(response, error)
      o:handleExtendedResponse(response, error)
    end
  )
end

function RaftServer:handleJoinClusterRequest(request)
  local logEntries = request.logEntries
  local response = {
    source = self.id,
    destination = request.source,
    term = self.state.term,
    messageType = RaftMessageType.JoinClusterResponse,
    nextIndex = self.logStore:getFirstAvailableIndex(),
    accepted = false
  }

  if
    (logEntries == nil or #logEntries ~= 1 or logEntries[1].valueType ~= LogValueType.Configuration or
      logEntries[1].value == nil or
      logEntries[1].value == 0)
   then
    self.logger.info("receive an invalid JoinClusterRequest as the log entry value doesn't meet the requirements")
    return response
  end

  if (self.catchingUp) then
    self.logger.info("this server is already in log syncing mode")
    return response
  end

  self.catchingUp = true
  self.role = ServerRole.Follower
  self.leader = request.source
  self.state.term = request.term
  self.state.commitIndex = 0
  self.quickCommitIndex = 0
  self.state.votedFor = -1
  self.context.serverStateManager:persistState(self.state)
  self:stopElectionTimer()
  local newConfig = ClusterConfiguration:fromBytes(logEntries[1].value)
  self:reconfigure(newConfig)
  response.term = self.state.term
  response.accepted = true

  local this = self
  response.callbackFunc = function()
    -- handle send error
    self.catchingUp = false
  end
  return response
end

function RaftServer:handleLeaveClusterRequest(request)
  local response = {
    source = self.id,
    destination = request.source,
    term = self.state.term,
    messageType = RaftMessageType.LeaveClusterResponse,
    nextIndex = self.logStore:getFirstAvailableIndex()
  }

  if (not self.configChanging) then
    self.steppingDown = 2
    response.accepted = true
  else
    response.accepted = false
  end

  return response
end

function RaftServer:removeServerFromCluster(serverId)
  local serverId = tonumber(serverId)
  local newConfig = ClusterConfiguration:new()
  newConfig.logIndex = self.logStore:getFirstAvailableIndex()
  newConfig.lastLogIndex = self.config.lastLogIndex
  -- config.servers is a sequence, we can not use this
  -- if newConfig.servers[serverId] then
  --     newConfig.servers[serverId] = nil
  -- end
  for _, server in ipairs(self.config.servers) do
    if serverId ~= server.id then
      newConfig.servers[#newConfig.servers + 1] = ClusterServer:new(server)
    end
  end

  -- self.logger.trace("removeServerFromCluster>peers:%s", util.table_tostring(self.peers))
  -- self.logger.trace("removeServerFromCluster>newConfig:%s", util.table_tostring(newConfig))
  self.logger.info(
    "removed a server from configuration and save the configuration to log store at %d",
    newConfig.logIndex
  )
  self.configChanging = true
  local newConfigBytes = newConfig:toBytes()

  -- self.logger.trace("removeServerFromCluster>newConfigBytes:%s", util.table_tostring(newConfigBytes))
  self.logStore:append(LogEntry:new(self.state.term, newConfig:toBytes(), LogValueType.Configuration))
  self:requestAllAppendEntries()
end

function RaftServer:getSnapshotSyncBlockSize()
  local blockSize = self.context.raftParameters.snapshotBlockSize
  return (blockSize == 0 and DEFAULT_SNAPSHOT_SYNC_BLOCK_SIZE) or blockSize
end

function RaftServer:createSyncSnapshotRequest(peer, lastLogIndex, term, commitIndex)
  local context = peer.snapshotSyncContext
  local snapshot
  if context then
    snapshot = context.snapshot
  end
  local lastSnapshot = self.stateMachine:getLastSnapshot()
  if (snapshot == nil or (lastSnapshot ~= nil and lastSnapshot.lastLogIndex > snapshot.lastLogIndex)) then
    snapshot = lastSnapshot

    if (snapshot == nil or lastLogIndex > snapshot.lastLogIndex) then
      self.logger.error(
        "system is running into fatal errors, failed to find a snapshot for peer %d(snapshot nil: %s, snapshot doesn't contains lastLogIndex: %s)",
        peer:getId(),
        (snapshot == nil and "true") or "false",
        (((snapshot == nil) or lastLogIndex > snapshot.lastLogIndex) and "true") or "false"
      )
      self.stateMachine:exit(-1)
      return nil
    end

    if (snapshot.size < 1) then
      self.logger.error(
        "invalid snapshot, this usually means a bug from state machine implementation, stop the system to prevent further errors"
      )
      self.stateMachine:exit(-1)
      return nil
    end

    self.logger.info("trying to sync snapshot with last index %d to peer %d", snapshot.lastLogIndex, peer:getId())
    peer.snapshotSyncContext = SnapshotSyncContext:new(snapshot)
  end

  local currentCollectionName = snapshot.collectionsNameSize[peer.snapshotSyncContext.currentCollectionIndex].name
  local offset = peer.snapshotSyncContext.offset
  local sizeLeft = snapshot.size - offset
  local blockSize = self:getSnapshotSyncBlockSize()
  local expectedSize = (sizeLeft > blockSize and blockSize) or sizeLeft
  local data = {}

  local sizeRead,
    error = self.stateMachine:readSnapshotData(snapshot, currentCollectionName, offset, data, expectedSize)
  if error then
    -- if there is i/o error, no reason to continue
    self.logger.error("failed to read snapshot data due to io error %s", error.toString())
    self.stateMachine:exit(-1)
    return nil
  end
  if (sizeRead and sizeRead < expectedSize) then
    self.logger.error(
      "only %d bytes could be read from snapshot while %d bytes are expected, should be something wrong",
      sizeRead,
      #data
    )
    self.stateMachine:exit(-1)
    return nil
  end

  local done =
    peer.snapshotSyncContext.currentCollectionIndex >= #snapshot.collectionsNameSize and
    (offset + expectedSize) >= snapshot.size
  local syncRequest = SnapshotSyncRequest:new(snapshot, offset, data, done, currentCollectionName)
  local requestMessage = {
    messageType = RaftMessageType.InstallSnapshotRequest,
    source = self.id,
    destination = peer:getId(),
    lastLogIndex = snapshot.lastLogIndex,
    lastLogTerm = snapshot.lastLogTerm,
    logEntries = {LogEntry:new(term, syncRequest:toBytes(), LogValueType.SnapshotSyncRequest)},
    commitIndex = commitIndex,
    term = term
  }
  return requestMessage
end

function RaftServer:termForLastLog(logIndex)
  if (logIndex == 0) then
    return 0
  end

  if (logIndex >= self.logStore:getStartIndex()) then
    return self.logStore:getLogEntryAt(logIndex).term
  end
  local lastSnapshot = self.stateMachine:getLastSnapshot()
  if (lastSnapshot == nil or logIndex ~= lastSnapshot.lastLogIndex) then
    self.logger.error("logIndex is beyond the range that no term could be retrieved")
    return
  end
  return lastSnapshot.lastLogTerm
end

function real_commit(server)
  local currentCommitIndex = server.state.commitIndex
  if server.quickCommitIndex <= currentCommitIndex or currentCommitIndex >= server.logStore:getFirstAvailableIndex() - 1 then
    return
  end
  while (currentCommitIndex < server.quickCommitIndex and
    currentCommitIndex < server.logStore:getFirstAvailableIndex() - 1) do
    currentCommitIndex = currentCommitIndex + 1
    server.logger.trace(
      "real_commit>currentCommitIndex:%d, quickCommitIndex:%d, FirstAvailableIndex:%d",
      currentCommitIndex,
      server.quickCommitIndex,
      server.logStore:getFirstAvailableIndex()
    )
    local logEntry = server.logStore:getLogEntryAt(currentCommitIndex)

    -- how can a LogEntry be nil ??
    -- perhaps on the start up, self commitIndex is -1, logStore's StartIndex(logStore:FirstAvailableIndex) is 1
    -- need more test here
    if logEntry == nil then
      -- do nothing
      server.logger.error("committed an empty LogEntry at %d !!", currentCommitIndex)
    elseif (logEntry.valueType == LogValueType.Application) then
      -- end
      -- if server.role ~= ServerRole.Leader then
      server.stateMachine:commit(currentCommitIndex, logEntry.value, server.role == ServerRole.Leader)
    elseif (logEntry.valueType == LogValueType.Configuration) then
      local newConfig = ClusterConfiguration:fromBytes(logEntry.value)
      server.logger.info("configuration at index %d is committed", newConfig.logIndex)
      server.context.serverStateManager:saveClusterConfiguration(newConfig)
      server.configChanging = false
      if (server.config.logIndex < newConfig.logIndex) then
        server:reconfigure(newConfig)
      end

      if (server.catchingUp and newConfig:getServer(server.id) ~= nil) then
        server.logger.info("this server is committed as one of cluster members")
        server.catchingUp = false
      end
    end

    server.state.commitIndex = currentCommitIndex
    server:snapshotAndCompact(currentCommitIndex)
  end

  server.context.serverStateManager:persistState(server.state)
end

local function activate()
  if (msg and msg.server) then
  end
end

NPL.this(activate)
