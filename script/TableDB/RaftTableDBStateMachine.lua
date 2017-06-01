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

NPL.load("(gl)script/ide/System/Database/TableDatabase.lua");
local TableDatabase = commonlib.gettable("System.Database.TableDatabase");

NPL.load("(gl)script/sqlite/libluasqlite3-loader.lua");
local api, ERR, TYPE, AUTH = load_libluasqlite3()

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
    -- NPL.load("(gl)script/ide/System/Database/IOThread.lua");
    -- local IOThread = commonlib.gettable("System.Database.IOThread");
    -- local collection = IOThread:GetSingleton():GetServerCollection(raftLogEntryValue.collection)

    -- --add to collections
    -- if raftLogEntryValue.collection and raftLogEntryValue.collection.name and not self.collections[raftLogEntryValue.collection.name] then
    --     local collectionPath = raftLogEntryValue.collection.db .. raftLogEntryValue.collection.name;
    --     self.logger.trace("add collection %s->%s", raftLogEntryValue.collection.name, collectionPath)
    --     self.collections[raftLogEntryValue.collection.name] = collectionPath;
    --     -- self.collections[raftLogEntryValue.collection.name] = collection
    -- end

    -- NPL.load("(gl)script/ide/System/Database/IORequest.lua");
    -- local IORequest = commonlib.gettable("System.Database.IORequest");
    -- -- a dedicated IOThread
    -- if raftLogEntryValue.query_type == "connect" then
    --     collection = {
    --         ToData = function (...)  end,
    --         GetWriterThreadName = function (...) return "main" end,
    --     }
    -- end
    -- IORequest:Send(raftLogEntryValue.query_type, collection, raftLogEntryValue.query,
    --     function (err, data)
    --         local msg = {
    --             err = err,
    --             data = data,
    --             cb_index = raftLogEntryValue.cb_index,
    --         }

    --         -- send Response
    --         -- we need handle active failure here
    --         RTDBRequestRPC(nil, raftLogEntryValue.serverId, msg)

    --     end);


    ---

    local cbFunc = function (err, data)
            local msg = {
                err = err,
                data = data,
                cb_index = raftLogEntryValue.cb_index,
            }

            -- send Response
            -- we need handle active failure here
            RTDBRequestRPC(nil, raftLogEntryValue.serverId, msg)
        end;

    -- a dedicated IOThread
    if raftLogEntryValue.query_type == "connect" and not self.db then
        self.db = TableDatabase:new():connect(raftLogEntryValue.query.rootFolder, cbFunc);
    else
        local collection = self.db[raftLogEntryValue.collection.name];
        if raftLogEntryValue.query.query then
            collection[raftLogEntryValue.query_type](collection, raftLogEntryValue.query.query,
                                                     raftLogEntryValue.query.update or raftLogEntryValue.query.replacement,
                                                     cbFunc);
        else
            collection[raftLogEntryValue.query_type](collection, raftLogEntryValue.query, cbFunc);
        end
    end

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
function RaftTableDBStateMachine:saveSnapshotData(snapshot, currentCollectionName, offset, data)
    local filePath = self.snapshotStore..string.format("%d-%d_%s_s.db", snapshot.lastLogIndex, snapshot.lastLogTerm, currentCollectionName);
    local snapshotConfPath = self.snapshotStore..string.format("%d.cnf", snapshot.lastLogIndex);

    if(not ParaIO.DoesFileExist(snapshotConfPath)) then
        local sConf = ParaIO.open(snapshotConfPath, "rw");
        local bytes = snapshot.lastConfig:toBytes();
        sConf:write(bytes, #bytes);
        sConf:close();
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
    for name,_ in pairs(self.collections) do
        local filePath = self.snapshotStore..string.format("%d-%d_%s_s.db", snapshot.lastLogIndex, snapshot.lastLogTerm, name);
        if(not ParaIO.DoesFileExist(filePath)) then
            return false;
        end
    end

    self.commitIndex = snapshot.lastLogIndex;
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
function RaftTableDBStateMachine:readSnapshotData(snapshot, currentCollectionName, offset, buffer, expectedSize)
    local filePath = self.snapshotStore..string.format("%d-%d_%s_s.db", snapshot.lastLogIndex, snapshot.lastLogTerm, currentCollectionName);
    self.logger.trace("readSnapshotData %s", filePath);
    if(not ParaIO.DoesFileExist(filePath)) then
        return -1;
    end

    local snapshotFile = ParaIO.open(filePath, "r");
    snapshotFile:seek(offset);
    snapshotFile:ReadBytes(expectedSize, buffer);
    snapshotFile:close();
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

    local collections = {};
    local collectionsSet = {};
    local maxLastLogIndex = 0;
    local maxTerm = 0
    local i;

    -- start from 0, inconsistent with lua
    for i = 0, nCount-1 do
        local filename = search_result:GetItem(i);
        local lastLogIndex, term, collection_name = string.match(filename,"(%d+)%-(%d+)_([%a+%d+]+)_s%.db");
        if not collectionsSet[collection_name] then
            collectionsSet[collection_name] = true;
            collections[#collections + 1] = collection_name
        end
        lastLogIndex = tonumber(lastLogIndex)
        term = tonumber(term)
        self.logger.trace("get file %s>lastLogIndex:%d, term:%d, collection:%s", filename, lastLogIndex, term, collection_name)
        if lastLogIndex > maxLastLogIndex then
            maxLastLogIndex = lastLogIndex;
            maxTerm = term;
            -- latestSnapshotFilename = filename;
        end
    end
    search_result:Release();

    if maxLastLogIndex > 0 then
        local snapshotConf = self.snapshotStore..string.format("%d.cnf", maxLastLogIndex);
        local sConf = ParaIO.open(snapshotConf, "r");
        if not sConf:IsValid() then
            self.logger.error("getLastSnapshot open %s err", snapshotConf)
            self:exit(-1);
        end
        local config = ClusterConfiguration:fromBytes(sConf:GetText(0, -1));
        sConf:close();
        -- get all collections snapshot total size
        local collectionsNameSize = {};
        for _,name in ipairs(collections) do
            local latestSnapshotFilename = format("%s%d-%d_%s_s.db", self.snapshotStore, maxLastLogIndex, maxTerm, name);
            self.logger.trace("latestSnapshot %s", latestSnapshotFilename)
            local latestSnapshot = ParaIO.open(latestSnapshotFilename, "r");
            if not latestSnapshot:IsValid() then
                self.logger.error("getLastSnapshot open %s err", latestSnapshotFilename)
                self:exit(-1);
            end
            local fileSize = latestSnapshot:GetFileSize();
            assert(fileSize>0, format("%s read error", latestSnapshotFilename));
            latestSnapshot:close();
            collectionsNameSize[#collectionsNameSize+1] = {name= name, size= fileSize};
        end
        if #collectionsNameSize > 0 then
            return Snapshot:new(maxLastLogIndex, maxTerm, config, collectionsNameSize[1].size, collectionsNameSize)
        else
            -- Raft help us to create a collection based on the snapshot
            return Snapshot:new(maxLastLogIndex, maxTerm, config)
        end
    end
end


function RaftTableDBStateMachine:getLatestSnapshotName(collection_name)
  -- list all files in the initial directory.
  local search_result = ParaIO.SearchFiles(self.snapshotStore, format("*%s_s.db", collection_name), "", 15, 10000, 0);
  local nCount = search_result:GetNumOfResult();

  local maxLastLogIndex = 0;
  local maxTerm = 0;

  for i = 0, nCount-1 do
      local filename = search_result:GetItem(i);
      local lastLogIndex, term, _ = string.match(filename,"(%d+)%-(%d+)_([%a+%d+]+)_s%.db");
      lastLogIndex = tonumber(lastLogIndex)
      term = tonumber(term)
      self.logger.trace("get file %s>lastLogIndex:%d, term:%d, collection:%s", filename, lastLogIndex, term, collection_name)
      if lastLogIndex > maxLastLogIndex then
          maxLastLogIndex = lastLogIndex;
          maxTerm = term;
      end
  end
  search_result:Release();

  return format("%s%d-%d_%s_s.db", self.snapshotStore, maxLastLogIndex, maxTerm, collection_name);
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

    -- make async ??

    local snapshotConf = self.snapshotStore..string.format("%d.cnf", snapshot.lastLogIndex);
    if(not ParaIO.DoesFileExist(snapshotConf)) then
        self.logger.trace("creating snapshot config:%s", snapshotConf);
        local sConf = ParaIO.open(snapshotConf, "rw");
        local bytes = snapshot.lastConfig:toBytes();
        sConf:WriteBytes(#bytes, {bytes:byte(1, -1)})
        sConf:close();
    end

    -- do backup here
    for name,collection in pairs(self.collections) do
        local filePath = self.snapshotStore..string.format("%d-%d_%s_s.db", snapshot.lastLogIndex, snapshot.lastLogTerm, name);
        local srcDBPath = collection..".db";
        self.logger.info("backing up %s to %s", name, filePath);

        -- local t = ParaIO.open(filePath, "rw");
        -- t:close();
        local backupDB = sqlite3.open(filePath)
        local srcDB = sqlite3.open(srcDBPath)
        -- local srcDB = collection.storageProvider._db
        -- backup can get error
        local bu = sqlite3.backup_init(backupDB, 'main', srcDB, 'main')

        local deleteBackupDB = false;
        local backupDBClosed = false;
        if bu then
            local stepResult = bu:step(-1);
            if stepResult ~= ERR.DONE then
                self.logger.error("back up failed")
                -- an error occured
                if stepResult == ERR.BUSY or stepResult == ERR.LOCKED then
                    -- we don't retry
                    bu:finish(); -- must free resource
                    backupDB:close()
                    backupDBClosed = true;
                    ParaIO.DeleteFile(filePath);
                    -- move the previous snapshot to current logIndex?
                    local prevSnapshortName = self:getLatestSnapshotName(name);
                    if prevSnapshortName == format("%s0-0_%s_s.db", self.snapshotStore, name) then
                        -- leave it
                        self.logger.error("backing up the 1st snapshot %s err", filePath)
                    else
                        self.logger.info("copy %s to %s", prevSnapshortName, filePath)
                        if not (ParaIO.CopyFile(prevSnapshortName, filePath, true)) then
                            self.logger.error("copy %s to %s failed", prevSnapshortName, filePath);
                            self:exit(-1);
                        end
                    end
                else
                    self.logger.error("must be a BUG!!!")
                    bu:finish();
                    self:exit(-1)
                end
            else
                bu:finish();
            end
        else
            -- A call to sqlite3_backup_init() will fail, returning NULL,
            -- if there is already a read or read-write transaction open on the destination database
            self.logger.error("backup_init failed")
            deleteBackupDB = true;
        end

        bu = nil;

        if not backupDBClosed then
            backupDB:close()
        end
        srcDB:close()
        if deleteBackupDB then
            ParaIO.DeleteFile(filePath);
        end
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
    -- frustrating
    -- C/C++ API call is counted as one instruction, so Exit does not block
    ParaGlobal.Exit(code)
    -- we don't want to go more
    ParaEngine.Sleep(1);
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