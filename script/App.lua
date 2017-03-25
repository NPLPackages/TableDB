--[[
Title: 
Author: 
Date: 
Desc: 
]]--

NPL.load("(gl)script/ide/commonlib.lua");
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
NPL.load("(gl)script/Raft/ServerState.lua");
NPL.load("(gl)script/Raft/ServerStateManager.lua");
NPL.load("(gl)script/Raft/RaftParameters.lua");
NPL.load("(gl)script/Raft/RaftContext.lua");
NPL.load("(gl)script/Raft/RpcListener.lua");
NPL.load("(gl)script/Raft/MessagePrinter.lua");
NPL.load("(gl)script/Raft/RaftClient.lua");

local RaftClient = commonlib.gettable("Raft.RaftClient");

-- local ServerState = commonlib.gettable("Raft.ServerState");
-- local ServerRole = NPL.load("(gl)script/Raft/ServerRole.lua");
local ServerStateManager = commonlib.gettable("Raft.ServerStateManager");
local RaftParameters = commonlib.gettable("Raft.RaftParameters");
local RaftContext = commonlib.gettable("Raft.RaftContext");
local RpcListener = commonlib.gettable("Raft.RpcListener");
NPL.load("(gl)script/ide/socket/url.lua");
local url = commonlib.gettable("commonlib.socket.url")
NPL.load("(gl)script/Raft/RaftConsensus.lua");
local RaftConsensus = commonlib.gettable("Raft.RaftConsensus");
NPL.load("(gl)script/Raft/RpcClient.lua");
local RpcClient = commonlib.gettable("Raft.RpcClient");
local MessagePrinter = commonlib.gettable("Raft.MessagePrinter");
local util = commonlib.gettable("System.Compiler.lib.util")
local LoggerFactory = NPL.load("(gl)script/Raft/LoggerFactory.lua");

local logger = LoggerFactory.getLogger("App")

-- local configDir = "script/config/"
-- local mpDir = "script/mpDir/"


local baseDir = ParaEngine.GetAppCommandLineByParam("baseDir", "./");
local mpPort = ParaEngine.GetAppCommandLineByParam("mpPort", "8090");
local raftMode = ParaEngine.GetAppCommandLineByParam("raftMode", "server");

logger.info("app arg:"..baseDir..mpPort..raftMode)

local stateManager = ServerStateManager:new(baseDir);
local config = stateManager:loadClusterConfiguration();

local localEndpoint = config:getServer(stateManager.serverId).endpoint
local parsed_url = url.parse(localEndpoint)
logger.info("local state info"..util.table_tostring(parsed_url))
local rpcListener = RpcListener:new(parsed_url.host, parsed_url.port, config.servers)

-- message printer
mp = MessagePrinter:new(baseDir, parsed_url.host, mpPort)

local function executeInServerMode(...)
    local raftParameters = RaftParameters:new()
    raftParameters.electionTimeoutUpperBound = 5000;
    raftParameters.electionTimeoutLowerBound = 3000;
    raftParameters.heartbeatInterval = 1500;
    raftParameters.rpcFailureBackoff = 500;
    raftParameters.maximumAppendingSize = 200;
    raftParameters.logSyncBatchSize = 5;
    raftParameters.logSyncStoppingGap = 5;
    raftParameters.snapshotEnabled = 5000;
    raftParameters.syncSnapshotBlockSize = 0;

    local context = RaftContext:new(stateManager,
                                    mp,
                                    raftParameters,
                                    rpcListener,
                                    LoggerFactory);
    RaftConsensus.run(context);
end


local function executeAsClient(localId, configuration, loggerFactory)
    local raftClient = RaftClient:new(localId, configuration, loggerFactory)

    local values = {
      "test:1111",
      "test:1112",
      "test:1113",
      "test:1114",
      "test:1115",
    }

    raftClient:appendEntries(values)
    
    -- while(true) do
    --     printf("Message:");

        -- for(int i = 1; i <= count; ++i){
        --     String msg = String.format(format, i);
        --     boolean accepted = client.appendEntries(new byte[][]{ msg.getBytes() }).get();
        --     printf("Accepted: " + String.valueOf(accepted));
        -- }


        -- boolean accepted = client.appendEntries(new byte[][]{ message.getBytes() }).get();
        -- printf("Accepted: " + String.valueOf(accepted));
    -- end
end

if raftMode:lower() == "server" then
  executeInServerMode()
elseif raftMode:lower() == "client" then
  NPL.StartNetServer("localhost", "9004");
  mp:start()
  executeAsClient(4, config, LoggerFactory)
end



local function activate()
  --  if(msg) then
      --- C/C++ API call is counted as one instruction, so if you call ParaEngine.Sleep(10), 
      --it will block all concurrent jobs on that NPL thread for 10 seconds
      -- ParaEngine.Sleep(0.5);
  --  end
end

NPL.this(activate);