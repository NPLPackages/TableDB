--[[
Title: remote procedure call
Author(s): LiXizhi@yeah.net
Date: 2017.03.25 2017/2/8
Desc: Create RPC in current NPL thread. Internally it will use an existing or a virtual NPL activation file that can be invoked from any thread.
use the lib:
------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/Rpc.lua");
local Rpc = commonlib.gettable("Raft.Rpc");
Rpc:new():init("Test.testRPC", function(self, msg)
LOG.std(nil, "info", "category", msg);
msg.output=true;
ParaEngine.Sleep(1);
return msg;
end)

Test.testRPC:MakePublic();

-- now we can invoke it anywhere in any thread or remote address.
Test.testRPC("(worker1)", {"input"}, function(err, msg)
assert(msg.output == true and msg[1] == "input")
echo(msg);
end);

-- time out in 500ms
Test.testRPC("(worker1)", {"input"}, function(err, msg)
assert(err == "timeout" and msg==nil)
echo(err);
end, 500);
------------------------------------------------------------
]]
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua")
local RaftMessageType = NPL.load("(gl)npl_mod/Raft/RaftMessageType.lua")
local util = commonlib.gettable("System.Compiler.lib.util")
local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua")

local logger = LoggerFactory.getLogger("Rpc")
local Rpc = commonlib.gettable("Raft.Rpc")

local rpc_instances = {}
local callbackQueue = {}

-- default time out for a given request. default to 10 seconds
Rpc.DefaultTimeout = 10000
-- internal timer period
Rpc.monitorPeriod = 10000

function Rpc:new(o)
  o = o or {}
  o.logger = logger
  o.thread_name = format("(%s)", __rts__:GetName())
  o.MaxWaitSeconds = 3
  o.request = {
    type = "run",
    callbackThread = o.thread_name
  }

  o.response = {
    type = "result"
  }

  setmetatable(o, Rpc)
  return o
end

function Rpc:OneTimeInit()
  if (self.inited) then
    return
  end
  self.inited = true
  NPL.load("(gl)script/ide/timer.lua")
  self.mytimer =
    commonlib.Timer:new(
    {
      callbackFunc = function(timer)
        self:CheckTimedOutRequests()
      end
    }
  )
  self.mytimer:Change(self.monitorPeriod, self.monitorPeriod)
end

-- remove any timed out request.
function Rpc:CheckTimedOutRequests()
  local curTime = ParaGlobal.timeGetTime()
  local timeout_pool
  for i, cb in pairs(callbackQueue) do
    if ((curTime - cb.startTime) > (cb.timeout or self.DefaultTimeout)) then
      timeout_pool = timeout_pool or {}
      timeout_pool[i] = cb
    end
  end
  if (timeout_pool) then
    for i, cb in pairs(timeout_pool) do
      callbackQueue[i] = nil
      if (cb.callbackFunc) then
        cb.callbackFunc("timeout", nil)
      end
    end
  end
end

local next_id = 0
function getNextId()
  next_id = next_id + 1
  return next_id
end
-- get next callback pool index. may return nil if max queue size is reached.
-- @return index or nil
function Rpc:PushCallback(callbackFunc, timeout)
  if (not callbackFunc) then
    return -1
  end
  local index = getNextId()
  callbackQueue[index] = {callbackFunc = callbackFunc, startTime = ParaGlobal.timeGetTime(), timeout = timeout}
  return index
end

function Rpc:PopCallback(index)
  if (index) then
    local cb = callbackQueue[index]
    if (cb) then
      callbackQueue[index] = nil
      return cb
    end
  end
end

-- @param funcName: global function name, such as "API.Auth"
-- @param handle_request_func: Rpc handler function of function(self, msg)  end
-- @param publicFileName: the activation file, if nil, it defaults to current file
function Rpc:init(funcName, handle_request_func, publicFileName)
  if rpc_instances[funcName] then
    return
  end
  self:SetFuncName(funcName)
  self.handle_request = handle_request_func or echo
  self:SetPublicFile(publicFileName)
  return self
end

-- @param funcName: global function name, such as "API.Auth"
function Rpc:SetFuncName(funcName)
  self.fullname = funcName
  self.filename = format("Rpc/%s.lua", self.fullname)
  if (commonlib.getfield(funcName)) then
    self.logger.warn("%s is overwritten", funcName)
  end
  commonlib.setfield(funcName, self)

  Rpc.AddInstance(funcName, self)
end

-- @param filename: the activation file, if nil, it defaults to current file
function Rpc:SetPublicFile(filename)
  filename = filename or format("Rpc/%s.lua", self.fullname)
  self.filename = filename

  -- if self.filename == "RaftRequestRPC" then
  --   NPL.this(function()
  --     self:OnActivated(msg);
  --   end, {PreemptiveCount = 2000, MsgQueueSize=50000, filename = self.filename});
  -- elseif self.filename == "RTDBRequestRPC" then
  --   NPL.this(function()
  --     self:OnActivated(msg);
  --   end, {PreemptiveCount = 1000, filename = self.filename});
  -- else
  NPL.this(
    function()
      self:OnActivated(msg)
    end,
    {filename = self.filename}
  )
  -- end
  self.logger.info("%s installed to file %s", self.fullname, self.filename)
end

function Rpc:__tostring()
  return format("%s: (Rpc defined in %s)", self.fullname or "", self.filename or "")
end

-- static
function Rpc.AddInstance(name, o)
  if (name) then
    rpc_instances[name] = o
  end
end

-- static
function Rpc.GetInstance(name)
  return rpc_instances[name or ""]
end

local added_runtime = {}
-- private: whenever a message arrives
function Rpc:OnActivated(msg)
  -- this is for tracing raft client
  -- if type(self.localAddress) == "table" then
    -- self.logger.trace("recv:")
    -- self.logger.trace(msg)
  -- end
  if (msg.tid) then
    -- unauthenticated? reject as early as possible or accept it.
    local messageType = msg.msg.messageType
    if (messageType) then
      local remoteAddress = msg.remoteAddress
      if
        messageType.int == RaftMessageType.ClientRequest.int or messageType.int == RaftMessageType.AddServerRequest.int or
          messageType.int == RaftMessageType.RemoveServerRequest.int
       then
        -- we got a client request
        if not added_runtime[remoteAddress.id] then
          added_runtime[remoteAddress.id] = true
          -- can not contain ':'
          local nid = string.sub(remoteAddress.id, 1, #remoteAddress.id - 1)
          -- local nid = "server" .. remoteAddress.id;
          self.logger.info("accepted nid is %s", nid)
          NPL.AddNPLRuntimeAddress({host = remoteAddress.host, port = remoteAddress.port, nid = nid})
        end
        RaftRequestRPCInit(nil, remoteAddress.id, {})
        NPL.accept(msg.tid, remoteAddress.id or "default_user")
        msg.nid = remoteAddress.id or "default_user"
        self.logger.info("connection %s is established and accepted as %s, a client request", msg.tid, msg.nid)
      else
        -- this must be Raft internal message exclude the above 3
        if msg.msg.source then
          remoteAddress = "server" .. msg.msg.source .. ":"
        end
        self.logger.trace("recv msg %s", util.table_tostring(msg))
        NPL.accept(msg.tid, remoteAddress or "default_user")
        msg.nid = remoteAddress or "default_user"
        self.logger.info("connection %s is established and accepted as %s, raft internal request", msg.tid, msg.nid)
      end
    else
      if msg.name then
        -- for client rsp in state machine
        NPL.accept(msg.tid, msg.tid)
        self.logger.info("connection %s is established and accepted as %s, client response", msg.tid, msg.nid)
      else
        self.logger.info("who r u? msg:%s", util.table_tostring(msg))
        NPL.reject(msg.tid)
      end
    end
  end

  if (msg.nid) then
    -- only respond to authenticated messages.
    if (msg.name) then
      local rpc_ = Rpc.GetInstance(msg.name)
      if (type(rpc_) == "table" and rpc_.OnActivated) then
        msg.name = nil
        return rpc_:OnActivated(msg)
      end
    end

    if (msg.type == "run") then
      local result, err = self:handle_request(msg.msg)
      if not result then
        self.logger.trace("result is null")
        return
      end
      if type(msg.remoteAddress) == "table" and msg.remoteAddress.id then
        msg.remoteAddress = msg.remoteAddress.id
      end
      -- here we could use msg.nid or msg.remoteAddress
      -- be clear about the nid!
      local vFileId = format("%s%s%s", msg.callbackThread, msg.nid, self.filename)
      self.response.name = self.fullname
      self.response.msg = result
      self.response.err = err
      self.response.remoteAddress = self.localAddress -- on the server side the local address is nil
      self.response.callbackId = msg.callbackId
      -- if type(self.localAddress) == "table" then
      -- self.logger.trace("activate on %s, msg:%s", vFileId, util.table_tostring(response))
      -- end
      local activate_result = NPL.activate(vFileId, self.response)

      if activate_result ~= 0 then
        if added_runtime[msg.nid] then
          activate_result = NPL.activate_with_timeout(self.MaxWaitSeconds, vFileId, self.response)
        end
        if activate_result ~= 0 then
          self.logger.error("activate on %s failed %d, msg type:%s", vFileId, activate_result, self.response.type)
          if result.callbackFunc then
            result.callbackFunc()
          end
        end
      end
    elseif (msg.type == "result" and msg.callbackId) then
      local cb = self:PopCallback(msg.callbackId)
      if (cb and cb.callbackFunc) then
        cb.callbackFunc(msg.err, msg.msg)
      end
    end
  end
end

-- smallest short value to avoid conflicts with manual id.
local min_short_value = 100000

-- by default, Rpc can only be called from threads in the current process.
-- in order to expose the API via NPL tcp protocol, one needs to call this function.
function Rpc:MakePublic()
  NPL.load("(gl)script/ide/System/Encoding/crc32.lua")
  local Encoding = commonlib.gettable("System.Encoding")
  local shortValue = Encoding.crc32(self.filename)
  if (shortValue < min_short_value) then
    shortValue = shortValue + min_short_value
  end
  NPL.AddPublicFile(self.filename, shortValue)
end

-- @param address: if nil, it is current NPL thread. it can be thread name like "(worker1)"
-- if NPL thread worker1 is not created, it will be automatically created.
-- Because NPL thread is reused, it is good practice to use only limited number of NPL threads per process.
-- for complete format, please see NPL.activate function.
-- @param msg: any table object
-- @param callbackFunc: result from the Rpc, function(err, msg) end
-- @param timeout:  time out in milliseconds. if nil, there is no timeout
-- if timed out callbackFunc("timeout", nil) is invoked on timeout
function Rpc:activate(localAddress, remoteAddress, msg, callbackFunc, timeout)
  -- local address = remoteAddress
  -- if(type(address) == "string") then
  --   local thread_name = address:match("^%((.+)%)$");
  --   if (thread_name) then
  --     self.workers = self.workers or {};
  --     if(not self.workers[address]) then
  --       self.workers[address] = true;
  --       NPL.CreateRuntimeState(thread_name, 0):Start();
  --     end
  --   end
  -- end
  -- thread_name = thread_name or "osAsync";
  -- NPL.CreateRuntimeState(thread_name, 0):Start();
  self.localAddress = localAddress
  self.remoteAddress = remoteAddress
  -- if type(self.localAddress) == "table" then
  --   self.localAddress.id = format("server%s:", self.localAddress.id);
  -- end
  -- TTL Cache
  self:OneTimeInit()
  local callbackId = self:PushCallback(callbackFunc)

  local vFileId = format("(%s)%s%s", self.remoteThread or "main", self.remoteAddress or "", self.filename)
  if string.match(self.remoteAddress, "%(%a+%)") then
    vFileId = format("%s%s", self.remoteAddress or "", self.filename)
  end
  self.request.msg = msg
  self.request.name = self.fullname
  self.request.callbackId = callbackId
  self.request.remoteAddress = self.localAddress
  -- if type(self.localAddress) == "table" then
  --   self.logger.trace("activate on %s, msg:%s", vFileId, util.table_tostring(msg))
  -- end
  local activate_result = NPL.activate(vFileId, self.request)
  -- handle memory leak
  if activate_result ~= 0 then
    -- activate_result = NPL.activate_with_timeout(self.MaxWaitSeconds, vFileId, msg)
    -- if activate_result ~= 0 then
    callbackQueue[callbackId] = nil
    self.logger.error("activate on %s failed %d, msg type:%s", vFileId, activate_result, msg.type)
  -- end
  end
  return activate_result
end

Rpc.__call = Rpc.activate
Rpc.__index = Rpc
