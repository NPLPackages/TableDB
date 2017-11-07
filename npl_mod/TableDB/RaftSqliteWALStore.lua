--[[
Title: RaftSqliteWALStore
Author(s): liuluheng,
Date: 2017/7/31
Desc:

use the lib:
------------------------------------------------------------
NPL.load("(gl)npl_mod/TableDB/RaftSqliteWALStore.lua");
local RaftSqliteWALStore = commonlib.gettable("TableDB.RaftSqliteWALStore");
------------------------------------------------------------
]]
NPL.load("(gl)script/ide/System/Database/StorageProvider.lua");
local StorageProvider = commonlib.gettable("System.Database.StorageProvider");
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)npl_mod/TableDB/RaftWALLogEntryValue.lua");
local RaftWALLogEntryValue = commonlib.gettable("TableDB.RaftWALLogEntryValue");
NPL.load("(gl)npl_mod/TableDB/RaftLogEntryValue.lua");
local RaftLogEntryValue = commonlib.gettable("TableDB.RaftLogEntryValue");

NPL.load("(gl)npl_mod/TableDB/RaftTableDBStateMachine.lua");
local RaftTableDBStateMachine = commonlib.gettable("TableDB.RaftTableDBStateMachine");
NPL.load("(gl)npl_mod/Raft/RaftClient.lua");
local RaftClient = commonlib.gettable("Raft.RaftClient");
local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua");

local RaftSqliteWALStore = commonlib.inherit(commonlib.gettable("System.Database.SqliteStore"), commonlib.gettable("TableDB.RaftSqliteWALStore"));

local callbackQueue = {};
local raftClient;

RaftSqliteWALStore.name = "raft";
RaftSqliteWALStore.thread_name = format("(%s)", __rts__:GetName());
RaftSqliteWALStore.logger = LoggerFactory.getLogger("RaftSqliteWALStore")

function RaftSqliteWALStore:createRaftClient(baseDir, host, port, id, threadName, rootFolder, useFile)
    RaftSqliteWALStore.responseThreadName = self.thread_name;
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
    
    local baseDir = baseDir or "./"
    local stateManager = ServerStateManager:new(baseDir);
    local config = stateManager:loadClusterConfiguration();
    
    local localAddress = {
        host = host or "localhost",
        port = port or "9004",
        id = id or "server4:",
    }
    
    if #localAddress.id < 4 then
        localAddress.id = format("server%s:", localAddress.id)
    end
    
    rtdb = RaftTableDBStateMachine:new(baseDir, threadName)
    -- NPL.StartNetServer(localAddress.host, localAddress.port);
    rtdb:start2(self)
    
    raftClient = RaftClient:new(localAddress, RTDBRequestRPC, config, LoggerFactory)
    
    self:connect(self, {rootFolder = rootFolder});


end

function RaftSqliteWALStore:setRaftClient(c)
    raftClient = c
end

function RaftSqliteWALStore:getRaftClient()
    return raftClient;
end

local entries = {}
function RaftSqliteWALStore:init(collection, init_args)
    print(util.table_tostring(init_args))
    RaftSqliteWALStore._super.init(self, collection);
    
    self.collection = collection;
    if not raftClient then
        self:createRaftClient(unpack(init_args));
    end
    
    local dbName = self.kFileName
    local this = self
    self._db:set_wal_page_hook(function(page_data, pgno, nTruncate, isCommit)
        local rootFolder = collection:GetParent():GetRootFolder();
        local collectionName = collection:GetName();
        local raftWALLogEntryValue = RaftWALLogEntryValue:new(rootFolder, collectionName, page_data, pgno, nTruncate, isCommit)
        
        local bytes = raftWALLogEntryValue:toBytes();
        entries[#entries + 1] = bytes;
        this.logger.info("%d entries, pgSize %d, pgno %d, nTruncate %d, isCommit %d", #entries, #page_data, pgno, nTruncate, isCommit);
        
        if isCommit == 1 then
            raftClient:appendEntries(entries, function(response, err)
                local result = (err == nil and response.accepted and "accepted") or "denied"
                if not (err == nil and response.accepted) then
                    this.logger.error("the %s WAL appendEntries request has been %s", collectionName, result)
                else
                    this.logger.debug("the %s WAL appendEntries request has been %s", collectionName, result)
                end
            -- if callbackFunc then
            -- 	callbackFunc(err);
            -- end
            end)
            entries = {}
        end
        return 1
    end)
    
    return self;
end


-- how many seconds to wait on busy database, before we send "queue_full" error. This parameter only takes effect when self.WaitOnBusyDB is true.
RaftSqliteWALStore.MaxWaitSeconds = 5;
-- default time out for a given request. default to 5 seconds
RaftSqliteWALStore.DefaultTimeout = 5000;
-- internal timer period
RaftSqliteWALStore.monitorPeriod = 5000;
-- true to log everything.
RaftSqliteWALStore.debug_log = false;

function RaftSqliteWALStore:OneTimeInit()
    if (self.inited) then
        return;
    end
    self.inited = true;
    NPL.load("(gl)script/ide/timer.lua");
    self.mytimer = commonlib.Timer:new({callbackFunc = function(timer)
        self:CheckTimedOutRequests();
    end})
-- self.mytimer:Change(self.monitorPeriod, self.monitorPeriod);
end

-- remove any timed out request.
function RaftSqliteWALStore:CheckTimedOutRequests()
    local curTime = ParaGlobal.timeGetTime();
    local timeout_pool;
    for i, cb in pairs(callbackQueue) do
        if ((curTime - cb.startTime) > (cb.timeout or self.DefaultTimeout)) then
            timeout_pool = timeout_pool or {};
            timeout_pool[i] = cb;
        end
    end
    if (timeout_pool) then
        for i, cb in pairs(timeout_pool) do
            callbackQueue[i] = nil;
            if (cb.callbackFunc) then
                cb.callbackFunc("timeout", nil);
            end
        end
    end
end

local next_id = 0;
function getNextId()
    next_id = next_id + 1;
    return next_id;
end
-- get next callback pool index. may return nil if max queue size is reached.
-- @return index or nil
function RaftSqliteWALStore:PushCallback(callbackFunc, timeout)
    -- if(not callbackFunc) then
    --   return -1;
    -- end
    local index = getNextId();
    callbackQueue[index] = {callbackFunc = callbackFunc, startTime = ParaGlobal.timeGetTime(), timeout = timeout};
    return index;
end

function RaftSqliteWALStore:PopCallback(index)
    if (index) then
        local cb = callbackQueue[index];
        if (cb) then
            callbackQueue[index] = nil;
            return cb;
        end
    end
end

-- return err, data.
function RaftSqliteWALStore:WaitForSyncModeReply(timeout, cb_index)
    timeout = timeout or self.DefaultTimeout;
    local thread = __rts__;
    local reply_msg;
    local startTime = ParaGlobal.timeGetTime();
    while (not reply_msg) do
        local nSize = thread:GetCurrentQueueSize();
        for i = 0, nSize - 1 do
            local msg = thread:PeekMessage(i, {filename = true});
            if (msg.filename == "Rpc/RTDBRequestRPC.lua" or msg.filename == "Rpc/ConnectRequestRPC.lua") then
                local msg = thread:PopMessageAt(i, {filename = true, msg = true});
                local out_msg = msg.msg;
                self.logger.trace("recv msg:%s", util.table_tostring(out_msg));
                -- we use this only in connect and we should ensure connect's cb_index should be -1
                if not RaftSqliteWALStore.EnableSyncMode then
                    raftClient.HandleResponse(nil, out_msg.msg);
                else
                    RaftSqliteWALStore:handleResponse(out_msg.msg);
                end
                if (cb_index and out_msg.msg and out_msg.msg.cb_index == cb_index) or
                    (cb_index == nil and out_msg.msg and out_msg.msg.destination and out_msg.msg.destination ~= -1) then
                    self.logger.debug("got the correct msg");
                    reply_msg = out_msg.msg;
                    break;
                end
            end
        end
        if ((ParaGlobal.timeGetTime() - startTime) > timeout) then
            LOG.std(nil, "warn", "RaftSqliteWALStore", "WaitForSyncModeReply TIMEOUT");
            return "timeout", nil;
        end
        if (reply_msg == nil) then
            if (ParaEngine.GetAttributeObject():GetField("HasClosingRequest", false) == true) then
                return "app_exit", nil;
            end
            if (thread:GetCurrentQueueSize() == nSize) then
                thread:WaitForMessage(nSize);
            end
        end
    end
    if (reply_msg) then
        return reply_msg.err, reply_msg.data;
    end
end


function RaftSqliteWALStore:handleResponse(msg)
    local cb = self:PopCallback(msg.cb_index);
    if (cb and cb.callbackFunc) then
        cb.callbackFunc(msg.err, msg.data);
    end
end


function RaftSqliteWALStore:GetCollection()
    return self.collection;
end


function RaftSqliteWALStore:connect(db, data, callbackFunc)
    local rootFolder = self:GetCollection():GetParent():GetRootFolder();
    local collectionName = self:GetCollection():GetName();
    local page_data = "not valid page data"
    local pgno = -1;
    local nTruncate = 1;
    local isCommit = 1;
    local raftWALLogEntryValue = RaftWALLogEntryValue:new(rootFolder, collectionName, page_data, pgno, nTruncate, isCommit)
    
    local bytes = raftWALLogEntryValue:toBytes();
    
    raftClient:setRequestRPC(ConnectRequestRPC)
    raftClient:appendEntries(bytes, function(response, err)
        local result = (err == nil and response.accepted and "accepted") or "denied"
        self.logger.info("the CONNECT request has been %s", result)
        if callbackFunc then
            callbackFunc(err, response.data);
        end
    end)
    
    self.logger.info("waiting for Connect");
    self:WaitForSyncModeReply(5000);
    raftClient:setRequestRPC(RTDBRequestRPC)

end
