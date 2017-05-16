--[[
Title: 
Author: liuluheng
Date: 2017.04.12
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/TableDB/RaftTableDBStateMachine.lua");
local RaftTableDBStateMachine = commonlib.gettable("TableDB.RaftTableDBStateMachine");
------------------------------------------------------------
]]--

NPL.load("(gl)script/Raft/ClusterConfiguration.lua");
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration");
NPL.load("(gl)script/Raft/Snapshot.lua");
local Snapshot = commonlib.gettable("Raft.Snapshot");
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)script/Raft/Rpc.lua");
local Rpc = commonlib.gettable("Raft.Rpc");
NPL.load("(gl)script/TableDB/RaftLogEntryValue.lua");
local RaftLogEntryValue = commonlib.gettable("TableDB.RaftLogEntryValue");

NPL.load("(gl)script/sqlite/sqlite3.lua");

local LoggerFactory = NPL.load("(gl)script/Raft/LoggerFactory.lua");
local RaftTableDBStateMachine = commonlib.gettable("TableDB.RaftTableDBStateMachine");

function RaftTableDBStateMachine:new(baseDir, ip, listeningPort, clientId) 
    local o = {
        ip = ip,
        port = listeningPort,
        clientId = clientId,
        logger = LoggerFactory.getLogger("RaftTableDBStateMachine"),
        snapshotStore = baseDir.."snapshot/",
        commitIndex = 0,
        messageSender = nil,
        snapshotInprogress = false,

        -- many collections??
        collections = {},

        messages = {},
        pendingMessages = {},
    };
    setmetatable(o, self);

    if not ParaIO.CreateDirectory(o.snapshotStore) then
        o.logger.error("%s dir create error", o.snapshotStore)
    end
    return o;
end

function RaftTableDBStateMachine:__index(name)
    return rawget(self, name) or RaftTableDBStateMachine[name];
end

function RaftTableDBStateMachine:__tostring()
    return util.table_tostring(self)
end


--[[
 * Starts the state machine, called by RaftConsensus, RaftConsensus will pass an instance of
 * RaftMessageSender for the state machine to send logs to cluster, so that all state machines
 * in the same cluster could be in synced
 * @param raftMessageSender
 ]]--
function RaftTableDBStateMachine:start(raftMessageSender)
    self.messageSender = raftMessageSender;
    
    local this = self;
    -- use Rpc for incoming Request message
    Rpc:new():init("RTDBRequestRPC", function(self, msg) 
        msg = this:processMessage(msg)
        return msg; 
    end)

    -- port is need to be string here??
    -- NPL.StartNetServer(self.ip, tostring(self.port));
    RTDBRequestRPC:MakePublic();

end



--[[
 * Starts the client side TableDB, called by RaftConsensus, RaftConsensus will pass an instance of
 * RaftMessageSender for the state machine to send logs to cluster, so that all state machines
 * in the same cluster could be in synced
 * @param raftMessageSender
 ]]--
function RaftTableDBStateMachine:start2(RaftSqliteStore)
    -- use Rpc for incoming Response message
    Rpc:new():init("RTDBRequestRPC", function(self, msg)
        print(format("Response:%s", util.table_tostring(msg)))
        RaftSqliteStore:handleResponse(msg)
    end)

    -- port is need to be string here??
    -- NPL.StartNetServer(self.ip, tostring(self.port));
    RTDBRequestRPC:MakePublic();

end
    
--[[
 * Commit the log data at the {@code logIndex}
 * @param logIndex the log index in the logStore
 * @param data 
 ]]--
function RaftTableDBStateMachine:commit(logIndex, data)
    -- data is logEntry.value
    local raftLogEntryValue = RaftLogEntryValue:fromBytes(data);
    print("commit value:"..util.table_tostring(raftLogEntryValue))
    NPL.load("(gl)script/ide/System/Database/IOThread.lua");
    local IOThread = commonlib.gettable("System.Database.IOThread");
    local collection = IOThread:GetSingleton():GetServerCollection(raftLogEntryValue.collection)

    --add to collections
    if raftLogEntryValue.collection and raftLogEntryValue.collection.name and not self.collections[raftLogEntryValue.collection.name] then
        -- self.collections[raftLogEntryValue.collection.name] = raftLogEntryValue.collection.db .. raftLogEntryValue.collection.name
        self.collections[raftLogEntryValue.collection.name] = collection
    end

    NPL.load("(gl)script/ide/System/Database/IORequest.lua");
    local IORequest = commonlib.gettable("System.Database.IORequest");
    -- a dedicated IOThread
    if raftLogEntryValue.query_type == "connect" then
        collection = { 
            ToData = function (...)  end,
            GetWriterThreadName = function (...) return "main" end,
        }
    end
    IORequest:Send(raftLogEntryValue.query_type, collection, raftLogEntryValue.query,
        function (err, data)
            local msg = {
                err = err,
                data = data,
                cb_index = raftLogEntryValue.cb_index,
            }

            -- send Response
            -- we need handle active failure here
            RTDBRequestRPC(nil, raftLogEntryValue.serverId, msg)

        end);

    self.commitIndex = logIndex;
end

--[[
 * Rollback a preCommit item at index {@code logIndex}
 * @param logIndex log index to be rolled back
 * @param data
 ]]--
function RaftTableDBStateMachine:rollback(logIndex, data)
    -- need more thought here
    -- rollback on last root pair
    -- self._db:exec("ROLLBACK");
end

--[[
 * PreCommit a log entry at log index {@code logIndex}
 * @param logIndex the log index to commit
 * @param data
 ]]--
function RaftTableDBStateMachine:preCommit(logIndex, data)
    -- add cb_index to response
    -- local raftLogEntryValue = RaftLogEntryValue:fromBytes(data);
    -- self.messages[raftLogEntryValue.cb_index] = raftLogEntryValue;
    -- self.pendingMessages[raftLogEntryValue.cb_index] = raftLogEntryValue;
end

--[[
 * Save data for the snapshot
 * @param snapshot the snapshot information
 * @param offset offset of the data in the whole snapshot
 * @param data part of snapshot data
 ]]--
function RaftTableDBStateMachine:saveSnapshotData(snapshot, offset, data)
    local filePath = self.snapshotStore..string.format("%d-%d_User_s.db", snapshot.lastLogIndex, snapshot.lastLogTerm);

    if(not ParaIO.DoesFileExist(filePath)) then
        local snapshotConf = self.snapshotStore..string.format("%d.cnf", snapshot.lastLogIndex);
        local sConf = ParaIO.open(snapshotConf, "rw");
        local bytes = snapshot.lastConfig:toBytes();
        sConf:write(bytes, #bytes);
    end

    local snapshotFile = ParaIO.open(filePath, "rw");
    snapshotFile:seek(offset);
    snapshotFile:WriteBytes(#data, data);

    snapshotFile:close();
end

--[[
 * Apply a snapshot to current state machine
 * @param snapshot
 * @return true if successfully applied, otherwise false
 

 how to handle multi collection snapshot data
 ]]--
function RaftTableDBStateMachine:applySnapshot(snapshot)
    local filePath = self.snapshotStore..string.format("%d-%d_User_s.db", snapshot.lastLogIndex, snapshot.lastLogTerm);
    if(not ParaIO.DoesFileExist(filePath)) then
        return false;
    end

    local snapshotFile = ParaIO.open(filePath, "r");

    self.commitIndex = snapshot.lastLogIndex;
    snapshotFile:close()

    return true;
end

--[[
 * Read snapshot data at the specified offset to buffer and return bytes read
 * @param snapshot the snapshot info
 * @param offset the offset of the snapshot data
 * @param buffer the buffer to be filled
 * @return bytes read


 how to handle multi collection snapshot data
 ]]--
function RaftTableDBStateMachine:readSnapshotData(snapshot, offset, buffer, expectedSize)
    local filePath = self.snapshotStore..string.format("%d-%d_User_s.db", snapshot.lastLogIndex, snapshot.lastLogTerm);
    if(not ParaIO.DoesFileExist(filePath)) then
        return -1;
    end

    local snapshotFile = ParaIO.open(filePath, "r");
    snapshotFile:seek(offset);
    snapshotFile:ReadBytes(expectedSize, buffer);
    return expectedSize;
end

--[[
 * Read the last snapshot information
 * @return last snapshot information in the state machine or null if none
 

 how to handle multi collection snapshot data
 ]]--
function RaftTableDBStateMachine:getLastSnapshot()
  -- list all files in the initial directory.
  local search_result = ParaIO.SearchFiles(self.snapshotStore, "*_s.db", "", 15, 10000, 0);
  local nCount = search_result:GetNumOfResult();
  
  local latestSnapshotFilename;
  local maxLastLogIndex = 0;
  local maxTerm = 0
  local i;
  
  -- start from 0, inconsistent with lua
  for i = 0, nCount-1 do 
    local filename = search_result:GetItem(i);
        local lastLogIndex, term = string.match(filename,"(%d+)%-(%d+)_(%a+)_s%.db");
        lastLogIndex = tonumber(lastLogIndex)
        term = tonumber(term)
        self.logger.trace("get file %s>lastLogIndex:%d, term:%d", filename, lastLogIndex, term)
        if lastLogIndex > maxLastLogIndex then
            maxLastLogIndex = lastLogIndex;
            maxTerm = term;
            latestSnapshotFilename = filename;
        end
  end
  search_result:Release();

  if latestSnapshotFilename then
      local snapshotConf = self.snapshotStore..string.format("%d.cnf", maxLastLogIndex);
      local sConf = ParaIO.open(snapshotConf, "r");
      local config = ClusterConfiguration:fromBytes(sConf:GetText(0, -1))
      local latestSnapshotFileSize = ParaIO.open(self.snapshotStore..latestSnapshotFilename, "r"):GetFileSize();
      return Snapshot:new(maxLastLogIndex, maxTerm, config, latestSnapshotFileSize)
  end

end

--[[
 * Create a snapshot data based on the snapshot information asynchronously
 * set the future to true if snapshot is successfully created, otherwise, 
 * set it to false
 * @param snapshot the snapshot info
 * @return true if snapshot is created successfully, otherwise false
 ]]--
function RaftTableDBStateMachine:createSnapshot(snapshot)
    self.logger.trace("createSnapshot: snapshot.lastLogIndex:%d, self.commitIndex:%d", snapshot.lastLogIndex, self.commitIndex);
    if(snapshot.lastLogIndex > self.commitIndex) then
        return false;
    end

    if self.snapshotInprogress then
        return false;
    end

    self.snapshotInprogress = true;
    local copyOfMessages = self.messages


    -- make async ??

    local snapshotConf = self.snapshotStore..string.format("%d.cnf", snapshot.lastLogIndex);
    if(not ParaIO.DoesFileExist(snapshotConf)) then
        self.logger.trace("creating snapshot config:%s", snapshotConf);
        local sConf = ParaIO.open(snapshotConf, "rw");
        local bytes = snapshot.lastConfig:toBytes();
        sConf:WriteBytes(#bytes, {bytes:byte(1, -1)})
    end

    -- do backup here
    for name,collection in pairs(self.collections) do
        local filePath = self.snapshotStore..string.format("%d-%d_%s_s.db", snapshot.lastLogIndex, snapshot.lastLogTerm, name);
        -- local srcDBPath = collection_name..".db";
        self.logger.info("backing up %s to %s", name, filePath);
        
        -- local t = ParaIO.open(filePath, "rw");
        -- t:close();
        local backupDB = sqlite3.open(filePath)
        -- local srcDB = sqlite3.open(srcDBPath)
        local srcDB = collection.storageProvider._db
        local bu = sqlite3.backup_init(backupDB, 'main', srcDB, 'main')
        bu:step(-1);
        bu:finish();
        bu = nil;

        backupDB:close()
        -- srcDB:close()
    end

    self.snapshotInprogress = false;

    return true;
end

--[[
 * Save the state of state machine to ensure the state machine is in a good state, then exit the system
 * this MUST exits the system to protect the safety of the algorithm
 * @param code 0 indicates the system is gracefully shutdown, -1 indicates there are some errors which cannot be recovered
 ]]--
function RaftTableDBStateMachine:exit(code)
    ParaGlobal.Exit(code)
end


function RaftTableDBStateMachine:processMessage(message)
    print("Got message " .. util.table_tostring(message));
    return self.messageSender:appendEntries(message);
end


function RaftTableDBStateMachine:addMessage(message)
    index = string.find(message, ':');
    if(index == nil ) then
        return;
    end
    
    key = string.sub(message, 1, index-1);
    self.messages[key] = message;
    self.pendingMessages[key] = nil;

end