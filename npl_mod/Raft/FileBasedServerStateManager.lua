--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/FileBasedServerStateManager.lua");
local FileBasedServerStateManager = commonlib.gettable("Raft.FileBasedServerStateManager");
------------------------------------------------------------
]] --

NPL.load("(gl)script/ide/Files.lua")
NPL.load("(gl)script/ide/Json.lua")
NPL.load("(gl)npl_mod/Raft/ClusterConfiguration.lua")
NPL.load("(gl)npl_mod/Raft/ServerState.lua")
NPL.load("(gl)npl_mod/Raft/FileBasedSequentialLogStore.lua")
local FileBasedSequentialLogStore = commonlib.gettable("Raft.FileBasedSequentialLogStore")
-- NPL.load("(gl)npl_mod/Raft/WALSequentialLogStore.lua");
-- local WALSequentialLogStore = commonlib.gettable("Raft.WALSequentialLogStore");
local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua")
local ServerState = commonlib.gettable("Raft.ServerState")
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration")

local FileBasedServerStateManager = commonlib.gettable("Raft.FileBasedServerStateManager")

local SequentialLogStore = FileBasedSequentialLogStore
local STATE_FILE = "server.state"
local CONFIG_FILE = "config.properties"
local CLUSTER_CONFIG_FILE = "cluster.json"

function FileBasedServerStateManager:new(dataDirectory)
  local o = {
    container = dataDirectory,
    logStore = SequentialLogStore:new(dataDirectory),
    logger = LoggerFactory.getLogger("FileBasedServerStateManager")
  }
  setmetatable(o, self)

  local configFile = ParaIO.open(o.container .. CONFIG_FILE, "r")
  if configFile:IsValid() then
    local line = configFile:readline()
    local index = string.find(line, "=")
    o.serverId = tonumber(string.sub(line, index + 1))
  end

  o.serverStateFileName = o.container .. STATE_FILE
  o.logger.info("started with stateFile:%s", o.serverStateFileName)
  o.serverStateFile = ParaIO.open(o.serverStateFileName, "rw")
  assert(o.serverStateFile:IsValid(), "serverStateFile not Valid")
  o.serverStateFile:seek(0)

  return o
end

function FileBasedServerStateManager:__index(name)
  return rawget(self, name) or FileBasedServerStateManager[name]
end

function FileBasedServerStateManager:__tostring()
  return util.table_tostring(self)
end

-- Load cluster configuration for this server
function FileBasedServerStateManager:loadClusterConfiguration()
  local filename = self.container .. CLUSTER_CONFIG_FILE
  local configFile = ParaIO.open(filename, "r")
  if configFile:IsValid() then
    local text = configFile:GetText()
    local config = commonlib.Json.Decode(text)
    return ClusterConfiguration:new(config)
  else
    self.logger.error("%s path error", filename)
  end
end

-- Save cluster configuration
function FileBasedServerStateManager:saveClusterConfiguration(configuration)
  local config = commonlib.Json.Encode(configuration)
  local filename = self.container .. CLUSTER_CONFIG_FILE
  local configFile = ParaIO.open(filename, "w")
  if configFile:IsValid() then
    configFile:WriteString(config)
    configFile:close()
  else
    self.logger.error("%s path error", filename)
  end
end

function FileBasedServerStateManager:persistState(serverState)
  self.logger.info(
    "persistState>term:%f,commitIndex:%f,votedFor:%f",
    serverState.term,
    serverState.commitIndex,
    serverState.votedFor
  )
  self.serverStateFile:WriteDouble(serverState.term)
  self.serverStateFile:WriteDouble(serverState.commitIndex)
  self.serverStateFile:WriteInt(serverState.votedFor)
  self.serverStateFile:SetEndOfFile()
  self.serverStateFile:seek(0)
end

function FileBasedServerStateManager:readState()
  self.serverStateFile:close()

  local serverStateFile = ParaIO.open(self.serverStateFileName, "r")
  if (serverStateFile:GetFileSize() == 0) then
    self.serverStateFile = ParaIO.open(self.serverStateFileName, "rw")
    assert(self.serverStateFile:IsValid(), "serverStateFile not Valid")
    self.serverStateFile:seek(0)
    self.logger.info("state file size == 0")
    return
  end

  local term = serverStateFile:ReadDouble()
  local commitIndex = serverStateFile:ReadDouble()
  local votedFor = serverStateFile:ReadInt()

  serverStateFile:close()

  self.serverStateFile = ParaIO.open(self.serverStateFileName, "rw")
  assert(self.serverStateFile:IsValid(), "serverStateFile not Valid")
  self.serverStateFile:seek(0)
  return ServerState:new(term, commitIndex, votedFor)
end

function FileBasedServerStateManager:close()
  self.serverStateFile:close()
  self.logStore:close()
end
