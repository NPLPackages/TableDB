--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 
]] --

NPL.load("(gl)script/ide/commonlib.lua")

local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua")

local logger = LoggerFactory.getLogger("TableDBClient")
local clientMode = ParaEngine.GetAppCommandLineByParam("clientMode", "appendEntries")
local serverId = tonumber(ParaEngine.GetAppCommandLineByParam("serverId", "5"))

local function executeAsClient()
  if clientMode == "appendEntries" then
    -- TestPerformance()
    -- TestBulkOperations()
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
    
    NPL.load("(gl)npl_mod/TableDB/test/test_TableDatabase.lua")
    -- TestInsertThroughputNoIndex()
    TestSQLOperations()
  else
    NPL.load("(gl)npl_mod/TableDB/RaftSqliteStore.lua")
    local RaftSqliteStore = commonlib.gettable("TableDB.RaftSqliteStore")
    local param = {
      baseDir = "./",
      host = "localhost",
      port = "9004",
      id = "4",
      threadName = "rtdb",
      rootFolder = "temp/test_raft_database",
      useFile = useFileStateManager
    }
    RaftSqliteStore:createRaftClient(
      param.baseDir,
      param.host,
      param.port,
      param.id,
      param.threadName,
      param.rootFolder,
      param.useFile
    )
    local raftClient = RaftSqliteStore:getRaftClient()

    if clientMode == "addServer" then
      local serverToJoin = {
        id = serverId,
        endpoint = "tcp://localhost:900" .. serverId
      }

      NPL.load("(gl)npl_mod/Raft/ClusterServer.lua")
      local ClusterServer = commonlib.gettable("Raft.ClusterServer")

      raftClient:addServer(
        ClusterServer:new(serverToJoin),
        function(response, err)
          local result = (err == nil and response.accepted and "accepted") or "denied"
          logger.info("the addServer request has been %s", result)
        end
      )
    elseif clientMode == "removeServer" then
      local serverIdToRemove = serverId
      raftClient:removeServer(
        serverIdToRemove,
        function(response, err)
          local result = (err == nil and response.accepted and "accepted") or "denied"
          logger.info("the removeServer request has been %s", result)
        end
      )
    else
      logger.error("unknown client command:%s", clientMode)
    end
  end
end

executeAsClient()

NPL.this(
  function()
  end
)
