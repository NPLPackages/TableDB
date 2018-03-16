--[[
Title:
Author: liuluheng
Date: 2017.03.25
Desc:
    FileBased is better, but without threadsafty

------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/SqliteBasedServerStateManager.lua");
local SqliteBasedServerStateManager = commonlib.gettable("Raft.SqliteBasedServerStateManager");
------------------------------------------------------------
]]
--
NPL.load("(gl)script/ide/Files.lua")
NPL.load("(gl)script/ide/Json.lua")
NPL.load("(gl)npl_mod/Raft/ClusterConfiguration.lua")
NPL.load("(gl)npl_mod/Raft/ServerState.lua")
NPL.load("(gl)script/sqlite/sqlite3.lua")

-- NPL.load("(gl)npl_mod/Raft/FileBasedSequentialLogStore.lua");
-- local FileBasedSequentialLogStore = commonlib.gettable("Raft.FileBasedSequentialLogStore");

NPL.load("(gl)npl_mod/Raft/WALSequentialLogStore.lua")
local WALSequentialLogStore = commonlib.gettable("Raft.WALSequentialLogStore")
local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua")
local ServerState = commonlib.gettable("Raft.ServerState")
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration")

local SqliteBasedServerStateManager = commonlib.gettable("Raft.SqliteBasedServerStateManager")

local SequentialLogStore = WALSequentialLogStore
local CONFIG_FILE = "config.properties"
local CLUSTER_CONFIG_FILE = "cluster.json"

function SqliteBasedServerStateManager:new(dataDirectory)
  local o = {
    container = dataDirectory,
    logStore = SequentialLogStore:new(dataDirectory),
    logger = LoggerFactory.getLogger("SqliteBasedServerStateManager")
  }
  setmetatable(o, self)
  local stateDBFileName = o.container .. "/serverState.db"
  o.logger.info("started with state DB:%s", stateDBFileName)
  o.db = sqlite3.open(stateDBFileName)
  o.db:exec("CREATE TABLE serverState (id INTEGER UNIQUE, term INTEGER, commitIndex INTEGER, votedFor INTEGER)")

  local configFile = ParaIO.open(o.container .. CONFIG_FILE, "r")
  if configFile:IsValid() then
    local line = configFile:readline()
    local index = string.find(line, "=")
    o.serverId = tonumber(string.sub(line, index + 1))
  end

  local insert = o.db:prepare("INSERT INTO serverState VALUES (:id, :term, :commitIndex, :votedFor)")

  insert:bind(o.serverId, -1, -1, -1)
  insert:exec()
  insert:close()

  return o
end

function SqliteBasedServerStateManager:__index(name)
  return rawget(self, name) or SqliteBasedServerStateManager[name]
end

function SqliteBasedServerStateManager:__tostring()
  return util.table_tostring(self)
end

-- Load cluster configuration for this server
function SqliteBasedServerStateManager:loadClusterConfiguration()
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
function SqliteBasedServerStateManager:saveClusterConfiguration(configuration)
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

function SqliteBasedServerStateManager:persistState(serverState)
  self.logger.trace(
    "persistState>term:%f,commitIndex:%f,votedFor:%f",
    serverState.term,
    serverState.commitIndex,
    serverState.votedFor
  )

  local update = self.db:prepare("UPDATE serverState Set term=?,commitIndex=?,votedFor=? WHERE id = ?")
  update:bind(serverState.term, serverState.commitIndex, serverState.votedFor, self.serverId)
  update:exec()
  update:close()
end

function SqliteBasedServerStateManager:readState()
  local stmt, err = self.db:prepare("SELECT term, commitIndex, votedFor FROM serverState WHERE id = " .. self.serverId)
  if stmt then
    local row = stmt:first_row()
    stmt:close()
    if row.term ~= -1 then
      return ServerState:new(row.term, row.commitIndex, row.votedFor)
    end
  end
end

function SqliteBasedServerStateManager:close()
  self.logStore:close()
end
