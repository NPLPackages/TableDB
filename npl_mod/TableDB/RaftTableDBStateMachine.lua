--[[
Title:
Author: liuluheng
Date: 2017.04.12
Desc:

------------------------------------------------------------
NPL.load("(gl)npl_mod/TableDB/RaftTableDBStateMachine.lua");
local RaftTableDBStateMachine = commonlib.gettable("TableDB.RaftTableDBStateMachine");
------------------------------------------------------------
]]
--
NPL.load("(gl)npl_mod/Raft/ClusterConfiguration.lua");
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration");
NPL.load("(gl)npl_mod/Raft/Snapshot.lua");
local Snapshot = commonlib.gettable("Raft.Snapshot");
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)npl_mod/Raft/Rpc.lua");
local Rpc = commonlib.gettable("Raft.Rpc");
NPL.load("(gl)npl_mod/TableDB/RaftLogEntryValue.lua");
local RaftLogEntryValue = commonlib.gettable("TableDB.RaftLogEntryValue");
NPL.load("(gl)npl_mod/TableDB/RaftWALLogEntryValue.lua");
local RaftWALLogEntryValue = commonlib.gettable("TableDB.RaftWALLogEntryValue");

NPL.load("(gl)script/ide/System/Database/TableDatabase.lua");
local TableDatabase = commonlib.gettable("System.Database.TableDatabase");

NPL.load("(gl)script/sqlite/libluasqlite3-loader.lua");
local api, ERR, TYPE, AUTH = load_libluasqlite3()

NPL.load("(gl)script/sqlite/sqlite3.lua");

local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua");
local RaftTableDBStateMachine = commonlib.gettable("TableDB.RaftTableDBStateMachine");

-- for function not in class
local logger = LoggerFactory.getLogger("RaftTableDBStateMachine");

function RaftTableDBStateMachine:new(baseDir, threadName)
    local o = {
        logger = LoggerFactory.getLogger("RaftTableDBStateMachine"),
        snapshotStore = baseDir .. "snapshot/",
        commitIndex = 0,
        messageSender = nil,
        MaxWaitSeconds = 5,
        latestCommand = -2,
        
        threadName = threadName,
        
        collections = {},
        
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
]]
--
function RaftTableDBStateMachine:start(raftMessageSender)
    self.messageSender = raftMessageSender;
    
    -- for init connect
    Rpc:new():init("RaftRequestRPCInit");
    RaftRequestRPCInit.remoteThread = self.threadName;
    RaftRequestRPCInit:MakePublic();

    local this = self;
    Rpc:new():init("RTDBRequestRPC", function(self, msg)
        return this:processMessage(msg)
    end)
    RTDBRequestRPC.remoteThread = self.threadName;
    RTDBRequestRPC:MakePublic();
    
    self.db = TableDatabase:new();
    -- start tdb
    -- NPL.load("(gl)script/ide/System/Database/IORequest.lua");
    -- local IORequest = commonlib.gettable("System.Database.IORequest");
    -- IORequest:Send("init_tdb", self.db);
    
    -- self.snapshotThread = "Snapshot";
    -- NPL.CreateRuntimeState(self.snapshotThread, 0):Start();
--     local this = self
--     Rpc:new():init("SnapshotRPC", function(self, msg)
--             this.logger.info(util.table_tostring(msg))
--             msg.output=true;
--             ParaEngine.Sleep(1);
--             return msg;
--         end)
--    SnapshotRPC("", "(Snapshot)", function (...)
--             this.logger.info(...)
--         end)
end



--[[
* Starts the client side TableDB, called by RaftConsensus, RaftConsensus will pass an instance of
* RaftSqliteStore for client to send logs to cluster
* @param RaftSqliteStore
]]
--
function RaftTableDBStateMachine:start2(RaftSqliteStore)
    -- for init connect
    Rpc:new():init("RaftRequestRPCInit");
    RaftRequestRPCInit.remoteThread = self.threadName;
    RaftRequestRPCInit:MakePublic();

    local this = self
    Rpc:new():init("RTDBRequestRPC", function(self, msg)
        this.logger.debug(format("Response:%s", util.table_tostring(msg)))
        RaftSqliteStore:handleResponse(msg)
    end)

    RTDBRequestRPC.remoteThread = self.threadName;
    RTDBRequestRPC:MakePublic();
    
    Rpc:new():init("ConnectRequestRPC", function(self, msg)
        this.logger.debug(format("Connect Response:%s", util.table_tostring(msg)))
        RaftSqliteStore:handleResponse(msg)
    end)

    ConnectRequestRPC.remoteThread = "rtdb";
    ConnectRequestRPC:MakePublic();

end


--[[
* Commit the log data at the {@code logIndex}
* @param logIndex the log index in the logStore
* @param data
]]
--
function RaftTableDBStateMachine:commit(logIndex, data)
    -- if logIndex % 10 == 0 then
        self.logger.info("commit:%d", logIndex);
    -- end
    
    local data = RaftWALLogEntryValue:fromBytes(data);
    data.logIndex = logIndex;
    
	local config_path = data.rootFolder .. "/tabledb.config.xml";
    if not ParaIO.DoesFileExist(config_path) then
        self:createSqliteWALStoreConfig(data.rootFolder);
        self.db:connect(data.rootFolder, function (...)
            self.logger.info("connected to %s", data.rootFolder);
        end);
    end

    --add to collections
    if not self.collections[data.collectionName] then
        local collectionPath = data.rootFolder .. data.collectionName;
        self.logger.trace("add collection %s->%s", data.collectionName, collectionPath)
        self.collections[data.collectionName] = collectionPath;
    end

    local collection = self.db[data.collectionName];
    collection:injectWALPage(data);

    self.commitIndex = logIndex;
end

--[[
* Rollback a preCommit item at index {@code logIndex}
* @param logIndex log index to be rolled back
* @param data
]]
--
function RaftTableDBStateMachine:rollback(logIndex, data)
-- need more thought here
-- rollback on last root pair
-- self._db:exec("ROLLBACK");
end

--[[
* PreCommit a log entry at log index {@code logIndex}
* @param logIndex the log index to commit
* @param data
]]
--
function RaftTableDBStateMachine:preCommit(logIndex, data)
    -- -- data is logEntry.value
    -- local raftLogEntryValue = RaftLogEntryValue:fromBytes(data);

    -- self.logger.info("preCommit:%s", util.table_tostring(raftLogEntryValue))

    -- local this = self;
    -- local cbFunc = function(err, data, re_exec)
    --     local msg = {
    --         err = err,
    --         data = data,
    --         cb_index = raftLogEntryValue.cb_index,
    --     }

    --     this.logger.trace("result:%s", util.table_tostring(msg))

    --     local remoteAddress = format("%s%s", raftLogEntryValue.callbackThread, raftLogEntryValue.serverId)
    --     if not re_exec then
    --         this.latestError = err;
    --         this.latestData = data;
    --     end
        
    --     RTDBRequestRPC(nil, remoteAddress, msg);
    -- end;

    -- if raftLogEntryValue.cb_index <= self.latestCommand then
    --     self.logger.info("got a retry msg, %d <= %d", raftLogEntryValue.cb_index, self.latestCommand);
    --     cbFunc(this.latestError, this.latestData, true);
    --     return;
    -- end
    
    -- -- a dedicated IOThread
    -- if raftLogEntryValue.query_type == "connect" then
    --     -- raftLogEntryValue.collection.db is nil when query_type is connect
    --     -- we should create table.config.xml here and make the storageProvider to SqliteWALStore
    --     self:createSqliteWALStoreConfig(raftLogEntryValue.query.rootFolder);
    --     self.db:connect(raftLogEntryValue.query.rootFolder, cbFunc);
    -- else
    --     if raftLogEntryValue.enableSyncMode then
    --         self.db:EnableSyncMode(true);
    --     end
    --     -- self.db:connect(raftLogEntryValue.collection.db);
    --     local collection = self.db[raftLogEntryValue.collection.name];
        
    --     --add to collections
    --     if not self.collections[raftLogEntryValue.collection.name] then
    --         local collectionPath = raftLogEntryValue.collection.db .. raftLogEntryValue.collection.name;
    --         self.logger.trace("add collection %s->%s", raftLogEntryValue.collection.name, collectionPath)
    --         self.collections[raftLogEntryValue.collection.name] = collectionPath;
    --     -- self.collections[raftLogEntryValue.collection.name] = collection
    --     end
        
    --     -- NOTE: this may not work when the query field named "update" or "replacement"
    --     if raftLogEntryValue.query.update or raftLogEntryValue.query.replacement then
    --         if raftLogEntryValue.enableSyncMode then
    --             cbFunc(collection[raftLogEntryValue.query_type](collection, raftLogEntryValue.query.query,
    --                 raftLogEntryValue.query.update or raftLogEntryValue.query.replacement));
    --         else
    --             collection[raftLogEntryValue.query_type](collection, raftLogEntryValue.query.query,
    --                 raftLogEntryValue.query.update or raftLogEntryValue.query.replacement,
    --                 cbFunc);
    --         end
    --     else
    --         if raftLogEntryValue.enableSyncMode then
    --             cbFunc(collection[raftLogEntryValue.query_type](collection, raftLogEntryValue.query));
    --         else
    --             collection[raftLogEntryValue.query_type](collection, raftLogEntryValue.query, cbFunc);
    --         end
    --     end
    -- end
    
    -- self.latestCommand = raftLogEntryValue.cb_index;
    -- self.commitIndex = logIndex;
end

--[[
* Save data for the snapshot
* @param snapshot the snapshot information
* @param offset offset of the data in the whole snapshot
* @param data part of snapshot data
]]
--
function RaftTableDBStateMachine:saveSnapshotData(snapshot, currentCollectionName, offset, data)
    local filePath = self.snapshotStore .. string.format("%d-%d_%s_s.db", snapshot.lastLogIndex, snapshot.lastLogTerm, currentCollectionName);
    local snapshotConfPath = self.snapshotStore .. string.format("%d.cnf", snapshot.lastLogIndex);
    
    if (not ParaIO.DoesFileExist(snapshotConfPath)) then
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
]]
--
function RaftTableDBStateMachine:applySnapshot(snapshot)
    for name, _ in pairs(self.collections) do
        local filePath = self.snapshotStore .. string.format("%d-%d_%s_s.db", snapshot.lastLogIndex, snapshot.lastLogTerm, name);
        if (not ParaIO.DoesFileExist(filePath)) then
            return false;
        end
    end
    
    self.commitIndex = snapshot.lastLogIndex;
    return true;
end

--[[
* Read snapshot data at the specified offset to buffer and return bytes read
* handle multi-collection snapshot data
* @param snapshot the snapshot info
* @param offset the offset of the snapshot data
* @param buffer the buffer to be filled
* @return bytes read
]]
--
function RaftTableDBStateMachine:readSnapshotData(snapshot, currentCollectionName, offset, buffer, expectedSize)
    local filePath = self.snapshotStore .. string.format("%d-%d_%s_s.db", snapshot.lastLogIndex, snapshot.lastLogTerm, currentCollectionName);
    self.logger.trace("readSnapshotData %s", filePath);
    if (not ParaIO.DoesFileExist(filePath)) then
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
* handle multi collection snapshot data
* @return last snapshot information in the state machine or null if none
]]
--
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
    for i = 0, nCount - 1 do
        local filename = search_result:GetItem(i);
        local lastLogIndex, term, collection_name = string.match(filename, "(%d+)%-(%d+)_([%a+%d+]+)_s%.db");
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
        local snapshotConf = self.snapshotStore .. string.format("%d.cnf", maxLastLogIndex);
        local sConf = ParaIO.open(snapshotConf, "r");
        if not sConf:IsValid() then
            self.logger.error("getLastSnapshot open %s err", snapshotConf)
            self:exit(-1);
        end
        local config = ClusterConfiguration:fromBytes(sConf:GetText(0, -1));
        sConf:close();
        -- get all collections snapshot total size
        local collectionsNameSize = {};
        for _, name in ipairs(collections) do
            local latestSnapshotFilename = format("%s%d-%d_%s_s.db", self.snapshotStore, maxLastLogIndex, maxTerm, name);
            self.logger.trace("latestSnapshot %s", latestSnapshotFilename)
            local latestSnapshot = ParaIO.open(latestSnapshotFilename, "r");
            if not latestSnapshot:IsValid() then
                self.logger.error("getLastSnapshot open %s err", latestSnapshotFilename)
                -- this may be caused by the snapshot db is backing up
                return;
            -- self:exit(-1);
            end
            local fileSize = latestSnapshot:GetFileSize();
            assert(fileSize > 0, format("%s read error", latestSnapshotFilename));
            latestSnapshot:close();
            collectionsNameSize[#collectionsNameSize + 1] = {name = name, size = fileSize};
        end
        if #collectionsNameSize > 0 then
            -- make snapshot size to the first collection snapshot size
            return Snapshot:new(maxLastLogIndex, maxTerm, config, collectionsNameSize[1].size, collectionsNameSize)
        else
            -- Raft help us to create a collection based on the snapshot
            return Snapshot:new(maxLastLogIndex, maxTerm, config)
        end
    end
end


local do_backup;
--[[
* Create a snapshot data based on the snapshot information (asynchronously)
* @param snapshot the snapshot info
* @return true if snapshot is created successfully, otherwise false
]]
--
function RaftTableDBStateMachine:createSnapshot(snapshot)
    self.logger.trace("createSnapshot: snapshot.lastLogIndex:%d, self.commitIndex:%d", snapshot.lastLogIndex, self.commitIndex);
    if (snapshot.lastLogIndex > self.commitIndex) then
        return false;
    end
    
    local snapshotConf = self.snapshotStore .. string.format("%d.cnf", snapshot.lastLogIndex);
    if (not ParaIO.DoesFileExist(snapshotConf)) then
        self.logger.trace("creating snapshot config:%s", snapshotConf);
        local sConf = ParaIO.open(snapshotConf, "rw");
        local bytes = snapshot.lastConfig:toBytes();
        sConf:WriteBytes(#bytes, {bytes:byte(1, -1)})
        sConf:close();
    end
    
    local msg = {
        snapshot = {lastLogIndex = snapshot.lastLogIndex, lastLogTerm = snapshot.lastLogTerm},
        collections = self.collections,
        snapshotStore = self.snapshotStore,
    }
    
    local success = true;
    -- local address = format("(%s)npl_mod/TableDB/RaftTableDBStateMachine.lua", self.snapshotThread);
    -- if (NPL.activate(address, msg) ~= 0) then
        -- in case of error
        -- if (NPL.activate_with_timeout(self.MaxWaitSeconds, address, msg) ~= 0) then
            -- self.logger.error("what's wrong with the snapshot thread?? we do backup in main")
            success = do_backup(msg, self);
        -- end
    -- end;
    
    return success;
end

--[[
* Save the state of state machine to ensure the state machine is in a good state, then exit the system
* this MUST exits the system to protect the safety of the algorithm
* @param code 0 indicates the system is gracefully shutdown, -1 indicates there are some errors which cannot be recovered
]]
--
function RaftTableDBStateMachine:exit(code)
    -- frustrating
    -- C/C++ API call is counted as one instruction, so Exit does not block
    ParaGlobal.Exit(code)
    -- we don't want to go more
    ParaEngine.Sleep(1);
end


function RaftTableDBStateMachine:processMessage(message)
    -- self.logger.info("Got a message");-- .. util.table_tostring(message));
    return self.messageSender:appendEntries(message);
end


function RaftTableDBStateMachine:createSqliteWALStoreConfig(rootFolder)
	NPL.load("(gl)script/ide/commonlib.lua");
	NPL.load("(gl)script/ide/LuaXML.lua");
	local config = { 
		name = "tabledb", 
		{
			name = "providers", 
			{ name = "provider", attr = { name = "sqliteWAL", type = "TableDB.SqliteWALStore", file = "(g1)npl_mod/TableDB/SqliteWALStore.lua" }, "" }
		},
		{
			name = "tables",
			{ name = "table", attr = { provider = "sqliteWAL", name = "default" } }, 
		}
	}

	local config_path = rootFolder .. "/tabledb.config.xml";
	local str = commonlib.Lua2XmlString(config, true);
	ParaIO.CreateDirectory(config_path);
	local file = ParaIO.open(config_path, "w");
	if (file:IsValid()) then
		file:WriteString(str);
		file:close();
	end
end


local function exit(code)
    -- C/C++ API call is counted as one instruction, so Exit does not block
    ParaGlobal.Exit(code)
    -- we don't want to go more
    ParaEngine.Sleep(1);
end


function getLatestSnapshotName(snapshotStore, collection_name)
    -- list all files in the initial directory.
    local search_result = ParaIO.SearchFiles(snapshotStore, format("*%s_s.db", collection_name), "", 15, 10000, 0);
    local nCount = search_result:GetNumOfResult();
    
    local maxLastLogIndex = 0;
    local maxTerm = 0;
    
    for i = 0, nCount - 1 do
        local filename = search_result:GetItem(i);
        local lastLogIndex, term, _ = string.match(filename, "(%d+)%-(%d+)_([%a+%d+]+)_s%.db");
        lastLogIndex = tonumber(lastLogIndex)
        term = tonumber(term)
        logger.trace("get file %s>lastLogIndex:%d, term:%d, collection:%s", filename, lastLogIndex, term, collection_name)
        if lastLogIndex > maxLastLogIndex then
            maxLastLogIndex = lastLogIndex;
            maxTerm = term;
        end
    end
    search_result:Release();
    
    return format("%s%d-%d_%s_s.db", snapshotStore, maxLastLogIndex, maxTerm, collection_name);
end


function do_backup(msg, stateMachine)
    local backup_success = true;
    local snapshot = msg.snapshot
    local collections = msg.collections
    local snapshotStore = msg.snapshotStore
    for name, collection in pairs(collections) do
        local filePath = snapshotStore .. string.format("%d-%d_%s_s.db", snapshot.lastLogIndex, snapshot.lastLogTerm, name);
        local srcDBPath = collection .. ".db";
        logger.info("backing up %s to %s", name, filePath);


        -- local originDB = stateMachine.db[name];
        -- originDB:close();
        
        local backupDB = sqlite3.open(filePath)
        local srcDB = sqlite3.open(srcDBPath)
        -- local srcDB = collection.storageProvider._db

        -- backup can get error
        local bu = sqlite3.backup_init(backupDB, 'main', srcDB, 'main')
        if bu then
            local stepResult = bu:step(-1);
            if stepResult ~= ERR.DONE then
                logger.error("back up failed")
                backup_success = false;

                -- an error occured
                if stepResult == ERR.BUSY or stepResult == ERR.LOCKED then
                    -- we don't retry, Raft will handle for us
                    -- move the previous snapshot to current logIndex?
                    -- local prevSnapshortName = getLatestSnapshotName(snapshotStore, name);
                    -- if prevSnapshortName == format("%s0-0_%s_s.db", snapshotStore, name) then
                    --     -- leave it
                    --     logger.error("backing up the 1st snapshot %s err", filePath)
                    -- else
                    --     logger.info("copy %s to %s", prevSnapshortName, filePath)
                    --     if not (ParaIO.CopyFile(prevSnapshortName, filePath, true)) then
                    --         logger.error("copy %s to %s failed", prevSnapshortName, filePath);
                    --         exit(-1);
                    --     end
                    -- end
                else
                    logger.error("must be a BUG!!!")
                    exit(-1)
                end
            end
            bu:finish();
        else
            -- A call to sqlite3_backup_init() will fail, returning NULL,
            -- if there is already a read or read-write transaction open on the destination database
            logger.error("backup_init failed")
            backup_success = false;
        end
        
        bu = nil;

        srcDB:close()
        backupDB:close()
        if not backup_success then
            ParaIO.DeleteFile(filePath);
            return backup_success;
        end
    end
    return backup_success;
end

local function activate()
    if (msg) then
        -- do backup here
        do_backup(msg)
    end
end

NPL.this(activate);
