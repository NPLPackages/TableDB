--[[
Title:
Author: liuluheng
Date: 2017.03.25
Desc:

Peer server in the same cluster for local server
this represents a peer for local server, it could be a leader, however, if local server is not a leader, though it has a list of peer servers, they are not used

------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/PeerServer.lua");
local PeerServer = commonlib.gettable("Raft.PeerServer");
------------------------------------------------------------
]]
--
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua")
local util = commonlib.gettable("System.Compiler.lib.util")

local RaftMessageType = NPL.load("(gl)npl_mod/Raft/RaftMessageType.lua")
local PeerServer = commonlib.gettable("Raft.PeerServer")

function PeerServer:new(server, ctx, heartbeatTimeoutHandler)
  local o = {
    clusterConfig = server,
    rpcClient = nil,
    currentHeartbeatInterval = ctx.raftParameters.heartbeatInterval,
    heartbeatInterval = ctx.raftParameters.heartbeatInterval,
    rpcBackoffInterval = ctx.raftParameters.rpcFailureBackoff,
    maxHeartbeatInterval = ctx.raftParameters:getMaxHeartbeatInterval(),
    busyFlag = 0,
    pendingCommitFlag = 0,
    heartbeatTimeoutHandler = heartbeatTimeoutHandler,
    nextLogIndex = 0,
    matchedIndex = 0,
    heartbeatEnabled = false,
    snapshotSyncContext = nil
  }

  o.heartbeatTask = function(timer)
    o.heartbeatTimeoutHandler(o)
  end
  o.heartbeatTimer = commonlib.Timer:new({callbackFunc = o.heartbeatTask})
  setmetatable(o, self)
  return o
end

function PeerServer:__index(name)
  return rawget(self, name) or PeerServer[name]
end

function PeerServer:__tostring()
  return util.table_tostring(self)
end

function PeerServer:toBytes()
  return
end

-- make sure this happens in one NPL thread(state)
function PeerServer:setFree()
  self.busyFlag = 0
end

function PeerServer:makeBusy()
  if self.busyFlag == 0 then
    self.busyFlag = 1
    return true
  end
  return false
end

-- make sure this happens in one NPL thread(state)
function PeerServer:setPendingCommit()
  self.pendingCommitFlag = 1
end

function PeerServer:clearPendingCommit()
  if self.pendingCommitFlag == 1 then
    self.pendingCommitFlag = 0
    return true
  end
  return false
end

function PeerServer:getId()
  return self.clusterConfig.id
end

function PeerServer:SendRequest(request, callbackFunc)
  local isAppendRequest =
    request.messageType == RaftMessageType.AppendEntriesRequest or
    request.messageType == RaftMessageType.InstallSnapshotRequest

  local o = self

	local source = request.source
  local destination = request.destination

  local function error_handler(msg, err, activate_result)
    o:slowDownHeartbeating()

    local err = {
      string = string.format("%s->%s failed(%d):%s", source, destination, activate_result or -1, err or ""),
      request = request
    }
    if isAppendRequest then
      err.string = err.string .. format(",commitIndex:%d", request.commitIndex)
    end

    if callbackFunc then
      callbackFunc(msg, err)
    end
  end

  local activate_result =
    RaftRequestRPC(
    source,
    destination,
    request,
    function(err, msg)
      if (isAppendRequest) then
        self:setFree()
      end
      if err then
        return error_handler(msg, err, 0)
      end
      o:resumeHeartbeatingSpeed()

      if callbackFunc then
        callbackFunc(msg, err)
      end
    end
  )
  if (activate_result ~= 0) then
    error_handler(nil, nil, activate_result)
  end
end

function PeerServer:slowDownHeartbeating()
  self.currentHeartbeatInterval =
    math.min(self.maxHeartbeatInterval, self.currentHeartbeatInterval + self.rpcBackoffInterval)
end

function PeerServer:resumeHeartbeatingSpeed()
  if (self.currentHeartbeatInterval > self.heartbeatInterval) then
    self.currentHeartbeatInterval = self.heartbeatInterval
  end
end
