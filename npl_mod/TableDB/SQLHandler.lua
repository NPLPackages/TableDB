--[[
Title:
Author: liuluheng
Date: 2017.04.12
Desc:

------------------------------------------------------------
NPL.load("(gl)npl_mod/TableDB/SQLHandler.lua");
local SQLHandler = commonlib.gettable("TableDB.SQLHandler");
------------------------------------------------------------
]]
--

NPL.load("(gl)script/ide/commonlib.lua");
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)npl_mod/Raft/Rpc.lua");
local Rpc = commonlib.gettable("Raft.Rpc");
NPL.load("(gl)npl_mod/TableDB/RaftLogEntryValue.lua");
local RaftLogEntryValue = commonlib.gettable("TableDB.RaftLogEntryValue");
NPL.load("(gl)npl_mod/Raft/ServerState.lua");
local ServerState = commonlib.gettable("Raft.ServerState");



NPL.load("(gl)script/ide/System/Database/TableDatabase.lua");
local TableDatabase = commonlib.gettable("System.Database.TableDatabase");

local RaftMessageType = NPL.load("(gl)npl_mod/Raft/RaftMessageType.lua");
local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua");
local SQLHandler = commonlib.gettable("TableDB.SQLHandler");

local g_threadName = __rts__:GetName();

-- for function not in class
local logger = LoggerFactory.getLogger("SQLHandler");

function SQLHandler:new(baseDir, useFile)
    local ServerStateManager;
    if useFile then
        NPL.load("(gl)npl_mod/Raft/FileBasedServerStateManager.lua");
        local FileBasedServerStateManager = commonlib.gettable("Raft.FileBasedServerStateManager");
        ServerStateManager = FileBasedServerStateManager;
    else
        NPL.load("(gl)npl_mod/Raft/SqliteBasedServerStateManager.lua");
        local SqliteBasedServerStateManager = commonlib.gettable("Raft.SqliteBasedServerStateManager");
        ServerStateManager = SqliteBasedServerStateManager;
    end

    local o = {
        baseDir = baseDir,
        stateManager = ServerStateManager:new(baseDir),
        monitorPeriod = 5000;
        threadName = g_threadName,
        logger = LoggerFactory.getLogger("SQLHandler"),
        latestCommand = -2,
        collections = {},
    };

    NPL.load("(gl)script/ide/timer.lua");
    o.mytimer = commonlib.Timer:new({callbackFunc = function(timer)
        o.logger.debug("reading server state")
        o.state = o.stateManager:readState();
    end})
    o.mytimer:Change(o.monitorPeriod, o.monitorPeriod);

    setmetatable(o, self);

    return o;
end

function SQLHandler:__index(name)
    return rawget(self, name) or SQLHandler[name];
end

function SQLHandler:__tostring()
    return util.table_tostring(self)
end

function SQLHandler:start()
    __rts__:SetMsgQueueSize(1000000);
    local this = self;
    Rpc:new():init("RaftRequestRPCInit");
    RaftRequestRPCInit.remoteThread = self.threadName;
    RaftRequestRPCInit:MakePublic();
    Rpc:new():init("RTDBRequestRPC", function(self, msg)
        msg = this:processMessage(msg)
        return msg;
    end)
    RTDBRequestRPC.remoteThread = self.threadName;
    RTDBRequestRPC:MakePublic();


    Rpc:new():init("ConnectRequestRPC", function(self, msg)
        msg = this:processMessage(msg)
        return msg;
    end)

    ConnectRequestRPC.remoteThread = self.threadName;
    ConnectRequestRPC:MakePublic();
    
    self.db = TableDatabase:new();

    self.logger.info("SQLHandler started");
end

function SQLHandler:processMessage(request)
    if not self.state then
        self.logger.debug("reading server state")
        self.state = self.stateManager:readState();
        if not self.state then
            return;
        end
    end

    local response = {
        messageType = RaftMessageType.AppendEntriesResponse,
        source = self.stateManager.serverId,
        destination = self.state.votedFor, -- use destination to indicate the leadId
        term = self.state.term,
    }
    
    if self.state.votedFor ~= self.stateManager.serverId then
        response.accepted = false
        return response
    end

    -- the leader executes the SQL, but the followers just append to WAL
    if request.logEntries and #request.logEntries > 0 then
        for i, v in ipairs(request.logEntries) do
            self:handle(v.value);
        end
    end
    
    response.accepted = true;
    return response
end


function SQLHandler:handle(data, callbackFunc)
    -- data is logEntry.value
    local raftLogEntryValue = RaftLogEntryValue:fromBytes(data);

    self.logger.trace("SQL:%s", util.table_tostring(raftLogEntryValue))

    local this = self;
    local cbFunc = function(err, data, re_exec)
        local msg = {
            err = err,
            data = data,
            cb_index = raftLogEntryValue.cb_index,
        }

        this.logger.trace("Result:%s", util.table_tostring(msg))

        local remoteAddress = format("%s%s", raftLogEntryValue.callbackThread, raftLogEntryValue.serverId)
        if not re_exec then
            this.latestError = err;
            this.latestData = data;
        end
        
        RTDBRequestRPC(nil, remoteAddress, msg);
    end;

    -- for test
    if callbackFunc then
        cbFunc = callbackFunc;
    end

    if raftLogEntryValue.cb_index <= self.latestCommand then
        self.logger.info("got a retry msg, %d <= %d", raftLogEntryValue.cb_index, self.latestCommand);
        cbFunc(this.latestError, this.latestData, true);
        return;
    end
    
    -- a dedicated IOThread
    if raftLogEntryValue.query_type == "connect" then
        -- raftLogEntryValue.collection.db is nil when query_type is connect
        -- we should create tabledb.config.xml here and make the storageProvider to SqliteWALStore
        self:createSqliteWALStoreConfig(raftLogEntryValue.query.rootFolder);
        self.db:connect(raftLogEntryValue.query.rootFolder, cbFunc);
    else
        if raftLogEntryValue.enableSyncMode then
            self.db:EnableSyncMode(true);
        end
        -- self.db:connect(raftLogEntryValue.collection.db);
        local collection = self.db[raftLogEntryValue.collectionName];
        
        --add to collections
        if not self.collections[raftLogEntryValue.collectionName] then
            local collectionPath = raftLogEntryValue.db .. raftLogEntryValue.collectionName;
            self.logger.trace("add collection %s->%s", raftLogEntryValue.collectionName, collectionPath)
            self.collections[raftLogEntryValue.collectionName] = collectionPath;
            -- self.db.collections:insertOne(nil, {name=raftLogEntryValue.collectionName,path=collectionPath});
        -- self.collections[raftLogEntryValue.collectionName] = collection
        end
        
        -- NOTE: this may not work when the query field named "update" or "replacement"
        if raftLogEntryValue.query.update or raftLogEntryValue.query.replacement then
            if raftLogEntryValue.enableSyncMode then
                cbFunc(collection[raftLogEntryValue.query_type](collection, raftLogEntryValue.query.query,
                    raftLogEntryValue.query.update or raftLogEntryValue.query.replacement));
            else
                collection[raftLogEntryValue.query_type](collection, raftLogEntryValue.query.query,
                    raftLogEntryValue.query.update or raftLogEntryValue.query.replacement,
                    cbFunc);
            end
        else
            if raftLogEntryValue.enableSyncMode then
                cbFunc(collection[raftLogEntryValue.query_type](collection, raftLogEntryValue.query));
            else
                collection[raftLogEntryValue.query_type](collection, raftLogEntryValue.query, cbFunc);
            end
        end
    end
    
    self.latestCommand = raftLogEntryValue.cb_index;
end

function SQLHandler:exit(code)
    -- frustrating
    -- C/C++ API call is counted as one instruction, so Exit does not block
    ParaGlobal.Exit(code)
    -- we don't want to go more
    ParaEngine.Sleep(1);
end

function SQLHandler:createSqliteWALStoreConfig(rootFolder)
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



local started = false;
local function activate()
    if(not started and msg and msg.start) then
        local sqlHandler = SQLHandler:new(msg.baseDir, msg.useFile);
        sqlHandler:start();
        started = true;
    end
end

NPL.this(activate);
