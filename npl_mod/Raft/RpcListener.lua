--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/RpcListener.lua");
local RpcListener = commonlib.gettable("Raft.RpcListener");
------------------------------------------------------------
]] --

NPL.load("(gl)npl_mod/Raft/Rpc.lua")
local Rpc = commonlib.gettable("Raft.Rpc")
NPL.load("(gl)script/ide/socket/url.lua")
local url = commonlib.gettable("commonlib.socket.url")
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua")
local util = commonlib.gettable("System.Compiler.lib.util")
local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua")
NPL.load("(gl)npl_mod/Raft/Rutils.lua")
local Rutils = commonlib.gettable("Raft.Rutils")

local RpcListener = commonlib.gettable("Raft.RpcListener")

function RpcListener:new(ip, port, serverId, servers, threadName)
  local o = {
    ip = ip,
    port = port,
    thisServerId = serverId,
    servers = servers,
    threadName = threadName,
    logger = LoggerFactory.getLogger("RpcListener")
  }

  for _, server in ipairs(o.servers) do
    Rutils.addServerToNPLRuntime(server)
  end

  setmetatable(o, self)
  return o
end

function RpcListener:__index(name)
  return rawget(self, name) or RpcListener[name]
end

function RpcListener:__tostring()
  return util.table_tostring(self)
end

--Starts listening and handle all incoming messages with messageHandler
--running in raft thread
function RpcListener:startListening(messageHandler)
  self.logger.info("startListening on %s:%s", self.ip, self.port)

  -- use Rpc for incoming Request message
  local this = self
  Rpc:new():init(
    "RaftRequestRPC",
    function(self, msg)
      -- this.logger.trace("RaftRequestRPC:%s",util.table_tostring(msg));
      msg = messageHandler:processRequest(msg)
      return msg
    end
  )
  RaftRequestRPC.remoteThread = self.threadName
  RaftRequestRPC:MakePublic()

  -- set NPL attributes before starting the server.
  local att = NPL.GetAttributeObject()
  att:SetField("TCPKeepAlive", true)
  -- att:SetField("KeepAlive", false);
  att:SetField("IdleTimeout", true)
  att:SetField("IdleTimeoutPeriod", 1200000)
  __rts__:SetMsgQueueSize(5000000)

  NPL.StartNetServer(self.ip, self.port)

  for _, server in ipairs(self.servers) do
    Rutils.initConnect(self.thisServerId, server)
  end
end
