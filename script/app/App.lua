--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 
]]--

NPL.load("(gl)script/ide/commonlib.lua");
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
NPL.load("(gl)script/Raft/ServerState.lua");
NPL.load("(gl)script/Raft/ServerStateManager.lua");
NPL.load("(gl)script/Raft/RaftParameters.lua");
NPL.load("(gl)script/Raft/RaftContext.lua");
NPL.load("(gl)script/Raft/RpcListener.lua");
NPL.load("(gl)script/app/MessagePrinter.lua");
NPL.load("(gl)script/Raft/RaftClient.lua");
NPL.load("(gl)script/ide/socket/url.lua");
NPL.load("(gl)script/Raft/RaftConsensus.lua");
NPL.load("(gl)script/Raft/RpcClient.lua");
NPL.load("(gl)script/Raft/ClusterServer.lua");
NPL.load("(gl)script/TableDB/RaftTableDBStateMachine.lua");
NPL.load("(gl)script/TableDB/RaftSqliteStore.lua");

local RaftSqliteStore = commonlib.gettable("TableDB.RaftSqliteStore");
local RaftTableDBStateMachine = commonlib.gettable("TableDB.RaftTableDBStateMachine");
local ClusterServer = commonlib.gettable("Raft.ClusterServer");
local RaftClient = commonlib.gettable("Raft.RaftClient");
local ServerStateManager = commonlib.gettable("Raft.ServerStateManager");
local RaftParameters = commonlib.gettable("Raft.RaftParameters");
local RaftContext = commonlib.gettable("Raft.RaftContext");
local RpcListener = commonlib.gettable("Raft.RpcListener");
local url = commonlib.gettable("commonlib.socket.url")
local RaftConsensus = commonlib.gettable("Raft.RaftConsensus");
local RpcClient = commonlib.gettable("Raft.RpcClient");
local MessagePrinter = commonlib.gettable("app.MessagePrinter");
local util = commonlib.gettable("System.Compiler.lib.util")
local LoggerFactory = NPL.load("(gl)script/Raft/LoggerFactory.lua");

local logger = LoggerFactory.getLogger("App")


local baseDir = ParaEngine.GetAppCommandLineByParam("baseDir", "");
local mpPort = ParaEngine.GetAppCommandLineByParam("mpPort", "8090");
local raftMode = ParaEngine.GetAppCommandLineByParam("raftMode", "server");
local clientMode = ParaEngine.GetAppCommandLineByParam("clientMode", "appendEntries");
local serverId = tonumber(ParaEngine.GetAppCommandLineByParam("serverId", "5"));

logger.info("app arg:"..baseDir..mpPort..raftMode)

local stateManager = ServerStateManager:new(baseDir);
local config = stateManager:loadClusterConfiguration();

logger.info("config:%s", util.table_tostring(config))

local thisServer = config:getServer(stateManager.serverId)
if not thisServer then
  -- perhaps thisServer has been removed last time
  ParaGlobal.Exit(0);
  --- C/C++ API call is counted as one instruction, so Exit does not block
  return;
end
local localEndpoint = thisServer.endpoint
local parsed_url = url.parse(localEndpoint)
logger.info("local state info"..util.table_tostring(parsed_url))
local rpcListener = RpcListener:new(parsed_url.host, parsed_url.port, thisServer.id, config.servers)

-- message printer
-- local mp = MessagePrinter:new(baseDir, parsed_url.host, mpPort)

-- raft stateMachine
local rtdb = RaftTableDBStateMachine:new(baseDir, parsed_url.host, mpPort)


local function executeInServerMode(stateMachine)
    local raftParameters = RaftParameters:new()
    raftParameters.electionTimeoutUpperBound = 5000;
    raftParameters.electionTimeoutLowerBound = 3000;
    raftParameters.heartbeatInterval = 1500;
    raftParameters.rpcFailureBackoff = 500;
    raftParameters.maximumAppendingSize = 200;
    raftParameters.logSyncBatchSize = 5;
    raftParameters.logSyncStopGap = 5;
    raftParameters.snapshotDistance = 5000;
    raftParameters.snapshotBlockSize = 0;

    local context = RaftContext:new(stateManager,
                                    stateMachine,
                                    raftParameters,
                                    rpcListener,
                                    LoggerFactory);
    RaftConsensus.run(context);
end


local function executeAsClient(localAddress, RequestRPC, configuration, loggerFactory)
    local raftClient = RaftClient:new(localAddress, RequestRPC, configuration, loggerFactory)
    RaftSqliteStore:setRaftClient(raftClient)

    if clientMode == "appendEntries" then
      NPL.load("(gl)script/TableDB/test/test_TableDatabase.lua");
      -- TestSQLOperations(RaftSqliteStore);
      -- TestInsertThroughputNoIndex(RaftSqliteStore)
      -- TestPerformance(RaftSqliteStore)
      -- TestBulkOperations(RaftSqliteStore)
      TestTimeout(RaftSqliteStore)

    
    elseif clientMode == "addServer" then
      local serverToJoin = {
        id = serverId,
        endpoint = "tcp://localhost:900"..serverId,
      }

      raftClient:addServer(ClusterServer:new(serverToJoin), function (response, err)
        local result = (err == nil and response.accepted and "accepted") or "denied"
        logger.info("the addServer request has been %s", result)
      end)
    
    elseif clientMode == "removeServer" then
      -- remove server
      local serverIdToRemove = serverId;
      raftClient:removeServer(serverIdToRemove, function (response, err)
        local result = (err == nil and response.accepted and "accepted") or "denied"
        logger.info("the removeServer request has been %s", result)
      end)
    else
      logger.error("unknown client command:%s", clientMode)
    end



end

if raftMode:lower() == "server" then
  -- executeInServerMode(mp)
  executeInServerMode(rtdb)
elseif raftMode:lower() == "client" then
  local localAddress = {
    host = "localhost",
    port = "9004",
    id = "server4:",
  }
  NPL.StartNetServer(localAddress.host, localAddress.port);
  -- mp:start()
  -- executeAsClient(localAddress, MPRequestRPC, config, LoggerFactory)
  rtdb:start2(RaftSqliteStore)
  executeAsClient(localAddress, RTDBRequestRPC, config, LoggerFactory)
end



local function activate()
   if(msg) then
      --- C/C++ API call is counted as one instruction, so if you call ParaEngine.Sleep(10), 
      --it will block all concurrent jobs on that NPL thread for 10 seconds
      -- ParaEngine.Sleep(0.5);
   end
end

NPL.this(function() end);