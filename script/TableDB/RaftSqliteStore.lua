--[[
Title: base class for store
Author(s): LiXizhi, 
Date: 2016/5/11
Desc: Derived class should implement at least following functions for the database store provider.
virtual functions:
	findOne
	find
	deleteOne
	updateOne
	insertOne
	removeIndex

use the lib:
------------------------------------------------------------
NPL.load("(gl)script/TableDB/RaftSqliteStore.lua");
local RaftSqliteStore = commonlib.gettable("TableDB.RaftSqliteStore");
------------------------------------------------------------
]]

NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)script/TableDB/RaftLogEntryValue.lua");
local RaftLogEntryValue = commonlib.gettable("TableDB.RaftLogEntryValue");
NPL.load("(gl)script/Raft/ServerStateManager.lua");
local ServerStateManager = commonlib.gettable("Raft.ServerStateManager");
NPL.load("(gl)script/TableDB/RaftTableDBStateMachine.lua");
local RaftTableDBStateMachine = commonlib.gettable("TableDB.RaftTableDBStateMachine");
NPL.load("(gl)script/Raft/RaftClient.lua");
local RaftClient = commonlib.gettable("Raft.RaftClient");
local LoggerFactory = NPL.load("(gl)script/Raft/LoggerFactory.lua");
local logger = LoggerFactory.getLogger("RaftSqliteStore")

local RaftSqliteStore = commonlib.inherit(commonlib.gettable("System.Database.Store"), commonlib.gettable("TableDB.RaftSqliteStore"));

local callbackQueue = {};


function RaftSqliteStore:createRaftClient()
	local baseDir = "./"
	local stateManager = ServerStateManager:new(baseDir);
	local config = stateManager:loadClusterConfiguration();

  local localAddress = {
    host = "localhost",
    port = "9004",
    id = "server4:",
  }

	rtdb = RaftTableDBStateMachine:new(baseDir, localAddress.host, localAddress.port, localAddress.id)
  rtdb:start2(self)
  self.raftClient = RaftClient:new(localAddress, RTDBRequestRPC, config, LoggerFactory)

end

function RaftSqliteStore:setRaftClient(raftClient)
	self.raftClient = raftClient
end

function RaftSqliteStore:ctor()
	self.stats = {
		select = 0,
		update = 0,
		insert = 0,
		delete = 0,
	};

	if not self.raftClient then
		self:createRaftClient()
	end

end

function RaftSqliteStore:init(collection)
	self.collection = collection;
	return self;
end



-- how many seconds to wait on busy database, before we send "queue_full" error. This parameter only takes effect when self.WaitOnBusyDB is true.
RaftSqliteStore.MaxWaitSeconds = 5;
-- default time out for a given request. default to 5 seconds
RaftSqliteStore.DefaultTimeout = 5000;
-- internal timer period
RaftSqliteStore.monitorPeriod = 5000;
-- true to log everything.
RaftSqliteStore.debug_log = false;

function RaftSqliteStore:OneTimeInit()
	if(self.inited) then
		return;
	end
	self.inited = true;
	NPL.load("(gl)script/ide/timer.lua");
	self.mytimer = commonlib.Timer:new({callbackFunc = function(timer)
		self:CheckTimedOutRequests();
	end})
	self.mytimer:Change(self.monitorPeriod, self.monitorPeriod);
end

-- remove any timed out request.
function RaftSqliteStore:CheckTimedOutRequests()
	local curTime = ParaGlobal.timeGetTime();
	local timeout_pool;
	for i, cb in pairs(callbackQueue) do
		if((curTime - cb.startTime) > (cb.timeout or self.DefaultTimeout) ) then
			timeout_pool = timeout_pool or {};
			timeout_pool[i] = cb;
		end
	end
	if(timeout_pool) then
		for i, cb in pairs(timeout_pool) do
			callbackQueue[i] = nil;
			if(cb.callbackFunc) then
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
function RaftSqliteStore:PushCallback(callbackFunc, timeout)
	if(not callbackFunc) then
		return -1;
	end
	local index = getNextId();
	callbackQueue[index] = {callbackFunc = callbackFunc, startTime = ParaGlobal.timeGetTime(), timeout=timeout};
	return index;
end

function RaftSqliteStore:PopCallback(index)
	if(index) then
		local cb = callbackQueue[index];
		if(cb) then
			callbackQueue[index] = nil;
			return cb;
		end
	end
end

-- return err, data.
function RaftSqliteStore:WaitForSyncModeReply(timeout)
	timeout = timeout or self.DefaultTimeout;
	local thread = __rts__;
	local reply_msg;
	local startTime = ParaGlobal.timeGetTime();
	while (not reply_msg) do
		local nSize = thread:GetCurrentQueueSize();
		for i=0, nSize-1 do
			local msg = thread:PeekMessage(i, {filename=true});
			if(msg.filename == "Rpc/RTDBRequestRPC.lua") then
				local msg = thread:PopMessageAt(i, {filename=true, msg=true});
				local out_msg = msg.msg;
				logger.debug("recv msg:%s", util.table_tostring(out_msg));
				-- we use this only in connect and we should ensure connect's cb_index should be -1
				self.raftClient.HandleResponse(nil, out_msg.msg);
				if(out_msg.cb_index == -1 or (out_msg.msg and out_msg.msg.destination ~= -1)) then
					logger.info("connect success!!")
					reply_msg = out_msg;
					break;
				end
			end
		end
		if( (ParaGlobal.timeGetTime() - startTime) > timeout) then
			LOG.std(nil, "warn", "RaftSqliteStore", "timed out");
			return "timeout", nil;
		end
		if(reply_msg == nil) then
			if(ParaEngine.GetAttributeObject():GetField("HasClosingRequest", false) == true) then
				return "app_exit", nil;
			end
			if(thread:GetCurrentQueueSize() == nSize) then
				thread:WaitForMessage(nSize);
			end
		end
	end
	if(reply_msg) then
		return reply_msg.err, reply_msg.data;
	end
end


function RaftSqliteStore:handleResponse(msg)
	local cb = self:PopCallback(msg.cb_index);
	if(cb and cb.callbackFunc) then
		cb.callbackFunc(msg.err, msg.data);
	end
end

-- called when a single command is finished. 
function RaftSqliteStore:CommandTick(commandname)
	if(commandname) then
		self:AddStat(commandname, 1);
	end
end

function RaftSqliteStore:GetCollection()
	return self.collection;
end

function RaftSqliteStore:GetStats()
	return self.stats;
end

-- add statistics for a given name
-- @param name: such as "select", "update", "insert", "delete"
-- @param count: if nil it is 1.
function RaftSqliteStore:AddStat(name, count)
	name = name or "unknown";
	local stats = self:GetStats();
	stats[name] = (stats[name] or 0) + (count or 1);
end

-- get current count for a given stats name
-- @param name: such as "select", "update", "insert", "delete"
function RaftSqliteStore:GetStat(name)
	name = name or "unknown";
	local stats = self:GetStats();
	return (stats[name] or 0);
end

function RaftSqliteStore:InvokeCallback(callbackFunc, err, data)
	if(callbackFunc) then
		callbackFunc(err, data);
	else
		return data;
	end
end


function RaftSqliteStore:connect(db, data, callbackFunc)
	local query_type = "connect"
  local collection = {
		ToData = function (...)	end,
  }

  local query = {
    rootFolder = data.rootFolder,
  }

  local raftLogEntryValue = RaftLogEntryValue:new(query_type, collection, query, -1);
  local bytes = raftLogEntryValue:toBytes();

	if not self.raftClient then
		self:createRaftClient()
	end

	self.raftClient:appendEntries(bytes, function (response, err)
      local result = (err == nil and response.accepted and "accepted") or "denied"
      logger.info("the %s request has been %s", query_type, result)
			if callbackFunc then
				callbackFunc(err, response.data);
			end
    end)

	self:WaitForSyncModeReply(10000);
end


function RaftSqliteStore:Send(query_type, query, callbackFunc)
	self:OneTimeInit();
	local index = self:PushCallback(callbackFunc);
	if(index) then
		local raftLogEntryValue = RaftLogEntryValue:new(query_type, self.collection, query,
																										index, self.raftClient.localAddress.id);
		local bytes = raftLogEntryValue:toBytes();

		self.raftClient:appendEntries(bytes, function (response, err)
				local result = (err == nil and response.accepted and "accepted") or "denied"
				logger.info("the %s request has been %s", query_type, result)
				-- if callbackFunc then
				-- 	callbackFunc(err);
				-- end
			end)
	end

end


-- virtual: 
-- please note, index will be automatically created for query field if not exist.
--@param query: key, value pair table, such as {name="abc"}
--@param callbackFunc: function(err, row) end, where row._id is the internal row id.
function RaftSqliteStore:findOne(query, callbackFunc)
	self:Send("findOne", query, callbackFunc)
end

-- virtual: 
-- find will not automatically create index on query fields. 
-- Use findOne for fast index-based search. This function simply does a raw search, if no index is found on query string.
-- @param query: key, value pair table, such as {name="abc"}. if nil or {}, it will return all the rows
-- @param callbackFunc: function(err, rows) end, where rows is array of rows found
function RaftSqliteStore:find(query, callbackFunc)
		self:Send("find", query, callbackFunc)
end

-- virtual: 
-- @param query: key, value pair table, such as {name="abc"}. 
-- @param callbackFunc: function(err, count) end
function RaftSqliteStore:deleteOne(query, callbackFunc)
		self:Send("deleteOne", query, callbackFunc)
end

-- virtual: delete multiple records
-- @param query: key, value pair table, such as {name="abc"}. 
-- @param callbackFunc: function(err, count) end
function RaftSqliteStore:delete(query, callbackFunc)
		self:Send("delete", query, callbackFunc)
end

-- virtual: 
-- this function will assume query contains at least one valid index key. 
-- it will not auto create index if key does not exist.
-- @param query: key, value pair table, such as {name="abc"}. 
-- @param update: additional fields to be merged with existing data; this can also be callbackFunc
function RaftSqliteStore:updateOne(query, update, callbackFunc)
		self:Send("updateOne", {query = query, update = update}, callbackFunc)
end

-- virtual: 
-- Replaces a single document within the collection based on the query filter.
-- it will not auto create index if key does not exist.
-- @param query: key, value pair table, such as {name="abc"}. 
-- @param replacement: wholistic fields to be replace any existing doc. 
function RaftSqliteStore:replaceOne(query, replacement, callbackFunc)
		self:Send("replaceOne", {query = query, replacement = replacement}, callbackFunc)
end


-- virtual: update multiple records, see also updateOne()
function RaftSqliteStore:update(query, update, callbackFunc)
		self:Send("update", {query = query, update = update}, callbackFunc)
end

-- virtual: 
-- if there is already one ore more records with query, this function falls back to updateOne().
-- otherwise it will insert and return full data with internal row _id.
-- @param query: nil or query fields. if it contains query fields, it will first do a findOne(), 
-- if there is record, this function actually falls back to updateOne. 
function RaftSqliteStore:insertOne(query, update, callbackFunc)
		self:Send("insertOne", {query = query, update = update}, callbackFunc)
end

-- virtual: 
-- counting the number of rows in a query. this will always do a table scan using an index. 
-- avoiding calling this function for big table. 
-- @param callbackFunc: function(err, count) end
function RaftSqliteStore:count(query, callbackFunc)
		self:Send("count", query, callbackFunc)
end

-- virtual: 
-- normally one does not need to call this function.
-- the store should flush at fixed interval.
-- @param callbackFunc: function(err, fFlushed) end
function RaftSqliteStore:flush(query, callbackFunc)
		self:Send("flush", query, callbackFunc)
end

-- virtual:
-- @param query: {"indexName"}
-- @param callbackFunc: function(err, bRemoved) end
function RaftSqliteStore:removeIndex(query, callbackFunc)
		self:Send("removeIndex", query, callbackFunc)
end


-- virtual:
-- after issuing an really important group of commands, and you want to ensure that 
-- these commands are actually successful like a transaction, the client can issue a waitflush 
-- command to check if the previous commands are successful. Please note that waitflush command 
-- may take up to 3 seconds or RaftSqliteStore.AutoFlushInterval to return. 
-- @param callbackFunc: function(err, fFlushed) end
function RaftSqliteStore:waitflush(query, callbackFunc, timeout)
		self:Send("waitflush", query, callbackFunc)
end

-- virtual:
-- this is usually used for changing database settings, such as cache size and sync mode. 
-- this function is specific to store implementation. 
-- @param query: string or {sql=string, CacheSize=number, IgnoreOSCrash=bool, IgnoreAppCrash=bool} 
function RaftSqliteStore:exec(query, callbackFunc)
		if(type(query) == "table") then
			-- also make the caller's message queue size twice as big at least
			if(query.QueueSize) then
				local value = query.QueueSize*2;
				if(__rts__:GetMsgQueueSize() < value) then
					__rts__:SetMsgQueueSize(value);
					LOG.std(nil, "system", "NPL", "NPL input queue size of thread (%s) is changed to %d", __rts__:GetName(), value);
				end
			end
			if(query.SyncMode~=nil) then
				RaftSqliteStore.EnableSyncMode = query.SyncMode;
				LOG.std(nil, "system", "TableDatabase", "sync mode api is %s in thread %s", query.SyncMode and "enabled" or "disabled", __rts__:GetName());
			end
		end

		self:Send("exec", query, callbackFunc)
end

-- virtual:
-- this function never reply. the client will always timeout
function RaftSqliteStore:silient(query)
		self:Send("silient", query, callbackFunc)
end

-- virtual: 
function RaftSqliteStore:makeEmpty(query, callbackFunc)
		self:Send("makeEmpty", query, callbackFunc)
end
