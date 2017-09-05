--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 
]]--

NPL.load("(gl)script/ide/commonlib.lua");
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
NPL.load("(gl)npl_mod/Raft/ServerState.lua");
NPL.load("(gl)npl_mod/Raft/FileBasedServerStateManager.lua");
NPL.load("(gl)npl_mod/Raft/RaftParameters.lua");
NPL.load("(gl)npl_mod/Raft/RaftContext.lua");
NPL.load("(gl)npl_mod/Raft/RpcListener.lua");
NPL.load("(gl)npl_mod/TableDBApp/MessagePrinter.lua");
NPL.load("(gl)npl_mod/Raft/RaftClient.lua");
NPL.load("(gl)script/ide/socket/url.lua");
NPL.load("(gl)npl_mod/Raft/RaftConsensus.lua");
NPL.load("(gl)npl_mod/Raft/ClusterServer.lua");
NPL.load("(gl)npl_mod/TableDB/RaftTableDBStateMachine.lua");

local RaftTableDBStateMachine = commonlib.gettable("TableDB.RaftTableDBStateMachine");
local ClusterServer = commonlib.gettable("Raft.ClusterServer");
local RaftClient = commonlib.gettable("Raft.RaftClient");
local FileBasedServerStateManager = commonlib.gettable("Raft.FileBasedServerStateManager");
local RaftParameters = commonlib.gettable("Raft.RaftParameters");
local RaftContext = commonlib.gettable("Raft.RaftContext");
local RpcListener = commonlib.gettable("Raft.RpcListener");
local url = commonlib.gettable("commonlib.socket.url")
local RaftConsensus = commonlib.gettable("Raft.RaftConsensus");
local MessagePrinter = commonlib.gettable("TableDBApp.MessagePrinter");
local util = commonlib.gettable("System.Compiler.lib.util")
local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua");

local logger = LoggerFactory.getLogger("App")


local threadName = ParaEngine.GetAppCommandLineByParam("threadName", "main");
local baseDir = ParaEngine.GetAppCommandLineByParam("baseDir", "");
-- local mpPort = ParaEngine.GetAppCommandLineByParam("mpPort", "8090");
local raftMode = ParaEngine.GetAppCommandLineByParam("raftMode", "server");
local clientMode = ParaEngine.GetAppCommandLineByParam("clientMode", "appendEntries");
local serverId = tonumber(ParaEngine.GetAppCommandLineByParam("serverId", "5"));

if threadName ~= "main" then
    NPL.CreateRuntimeState(threadName, 0):Start();
end

logger.info("app arg:"..baseDir.." "..raftMode)

local stateManager = FileBasedServerStateManager:new(baseDir);
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

-- message printer
-- local mp = MessagePrinter:new(baseDir, parsed_url.host, mpPort)


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

    local rpcListener = RpcListener:new(parsed_url.host, parsed_url.port, thisServer.id, config.servers, threadName)
    local context = RaftContext:new(stateManager,
                                    stateMachine,
                                    raftParameters,
                                    rpcListener,
                                    LoggerFactory);
    RaftConsensus.run(context);
end


local function executeAsClient()

    if clientMode == "appendEntries" then
      NPL.load("(gl)npl_mod/TableDB/test/test_TableDatabase.lua");
      -- TestSQLOperations();
      -- TestInsertThroughputNoIndex()
      -- TestPerformance()
      TestBulkOperations()
      -- TestTimeout()
      -- TestBlockingAPI()
      -- TestBlockingAPILatency()
      -- TestConnect()
      -- TestRemoveIndex()
      -- TestTable()
      -- TestTableDatabase();
      -- TestRangedQuery();
      -- TestPagination()
      -- TestCompoundIndex()
      -- TestCountAPI()
      -- TestDelete()
    
    else
      NPL.load("(gl)npl_mod/TableDB/RaftSqliteStore.lua");
      local RaftSqliteStore = commonlib.gettable("TableDB.RaftSqliteStore");
      RaftSqliteStore:createRaftClient()
      local raftClient = RaftSqliteStore:getRaftClient();

      if clientMode == "addServer" then
        local serverToJoin = {
          id = serverId,
          endpoint = "tcp://localhost:900"..serverId,
        }

        raftClient:addServer(ClusterServer:new(serverToJoin), function (response, err)
          local result = (err == nil and response.accepted and "accepted") or "denied"
          logger.info("the addServer request has been %s", result)
        end)
      
      elseif clientMode == "removeServer" then
        local serverIdToRemove = serverId;
        raftClient:removeServer(serverIdToRemove, function (response, err)
          local result = (err == nil and response.accepted and "accepted") or "denied"
          logger.info("the removeServer request has been %s", result)
        end)
      else
        logger.error("unknown client command:%s", clientMode)
      end
    end
end

local vfileID = format("(%s)npl_mod/TableDBApp/App.lua", threadName);
NPL.activate(vfileID, {start = true});


local started = false;
local function activate()
  if(not started and msg and msg.start) then
    started = true;
    -- raft stateMachine
    logger.info("start stateMachine");
    local rtdb = RaftTableDBStateMachine:new(baseDir, parsed_url.host, mpPort, threadName)
    if raftMode:lower() == "server" then
      -- executeInServerMode(mp)
      executeInServerMode(rtdb)
    elseif raftMode:lower() == "client" then
      executeAsClient()
    end
  end
end

NPL.this(activate);

-- NPL.this(function() end);