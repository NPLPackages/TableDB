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

NPL.load("(gl)script/Raft/RaftClient.lua");
local RaftClient = commonlib.gettable("Raft.RaftClient");


local LoggerFactory = NPL.load("(gl)script/Raft/LoggerFactory.lua");


local RaftSqliteStore = commonlib.inherit(commonlib.gettable("System.Database.Store"), commonlib.gettable("TableDB.RaftSqliteStore"));

function RaftSqliteStore:ctor()
	self.stats = {
		select = 0,
		update = 0,
		insert = 0,
		delete = 0,
	};


	local baseDir = "./"
	local stateManager = ServerStateManager:new(baseDir);
	local config = stateManager:loadClusterConfiguration();

	rtdb = RaftTableDBStateMachine:new(baseDir)

  local localAddress = {
    host = "localhost",
    port = "9004",
    id = "server4:",
  }
  NPL.StartNetServer(localAddress.host, localAddress.port);
	-- only for RTDBRequestRPC can be used
  rtdb:start()

  self.raftClient = RaftClient:new(localAddress, RTDBRequestRPC, config, LoggerFactory)

end

function RaftSqliteStore:init(collection)
	self.collection = collection;
	return self;
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


function RaftSqliteStore:connnect(db, data, callbackFunc)
	local query_type = "connect"
  local collection = {
		ToData = function (...)	end,
  }

  local query = {
    rootFolder = data.rootFolder,
  }

  local raftLogEntryValue = RaftLogEntryValue:new(query_type, collection, query);
  local bytes = raftLogEntryValue:toBytes();

	self.raftClient:appendEntries(bytes, function (response, err)
        local result = (err == nil and response.accepted and "accepted") or "denied"
        logger.info("the appendEntries request has been %s", result)
				if callbackFunc then
					callbackFunc();
				end
      end)

end


-- virtual: 
-- please note, index will be automatically created for query field if not exist.
--@param query: key, value pair table, such as {name="abc"}
--@param callbackFunc: function(err, row) end, where row._id is the internal row id.
function RaftSqliteStore:findOne(query, callbackFunc)
	return self:InvokeCallback(callbackFunc, "NotImplemented", nil);
end

-- virtual: 
-- find will not automatically create index on query fields. 
-- Use findOne for fast index-based search. This function simply does a raw search, if no index is found on query string.
-- @param query: key, value pair table, such as {name="abc"}. if nil or {}, it will return all the rows
-- @param callbackFunc: function(err, rows) end, where rows is array of rows found
function RaftSqliteStore:find(query, callbackFunc)
	return self:InvokeCallback(callbackFunc, "NotImplemented", nil);
end

-- virtual: 
-- @param query: key, value pair table, such as {name="abc"}. 
-- @param callbackFunc: function(err, count) end
function RaftSqliteStore:deleteOne(query, callbackFunc)
	return self:InvokeCallback(callbackFunc, "NotImplemented", nil);
end

-- virtual: delete multiple records
-- @param query: key, value pair table, such as {name="abc"}. 
-- @param callbackFunc: function(err, count) end
function RaftSqliteStore:delete(query, callbackFunc)
	return self:InvokeCallback(callbackFunc, "NotImplemented", nil);
end

-- virtual: 
-- this function will assume query contains at least one valid index key. 
-- it will not auto create index if key does not exist.
-- @param query: key, value pair table, such as {name="abc"}. 
-- @param update: additional fields to be merged with existing data; this can also be callbackFunc
function RaftSqliteStore:updateOne(query, update, callbackFunc)
	return self:InvokeCallback(callbackFunc, "NotImplemented", nil);
end

-- virtual: 
-- Replaces a single document within the collection based on the query filter.
-- it will not auto create index if key does not exist.
-- @param query: key, value pair table, such as {name="abc"}. 
-- @param replacement: wholistic fields to be replace any existing doc. 
function RaftSqliteStore:replaceOne(query, replacement, callbackFunc)
	return self:InvokeCallback(callbackFunc, "NotImplemented", nil);
end


-- virtual: update multiple records, see also updateOne()
function RaftSqliteStore:update(query, update, callbackFunc)
	return self:InvokeCallback(callbackFunc, "NotImplemented", nil);
end

-- virtual: 
-- if there is already one ore more records with query, this function falls back to updateOne().
-- otherwise it will insert and return full data with internal row _id.
-- @param query: nil or query fields. if it contains query fields, it will first do a findOne(), 
-- if there is record, this function actually falls back to updateOne. 
function RaftSqliteStore:insertOne(query, update, callbackFunc)
	return self:InvokeCallback(callbackFunc, "NotImplemented", nil);
end

-- virtual: 
-- counting the number of rows in a query. this will always do a table scan using an index. 
-- avoiding calling this function for big table. 
-- @param callbackFunc: function(err, count) end
function RaftSqliteStore:count(query, callbackFunc)
	return self:InvokeCallback(callbackFunc, "NotImplemented", nil);
end

-- virtual: 
-- normally one does not need to call this function.
-- the store should flush at fixed interval.
-- @param callbackFunc: function(err, fFlushed) end
function RaftSqliteStore:flush(query, callbackFunc)
	return self:InvokeCallback(callbackFunc, "NotImplemented", nil);
end

-- virtual:
-- @param query: {"indexName"}
-- @param callbackFunc: function(err, bRemoved) end
function RaftSqliteStore:removeIndex(query, callbackFunc)
	return self:InvokeCallback(callbackFunc, "NotImplemented", nil);
end


-- virtual:
-- after issuing an really important group of commands, and you want to ensure that 
-- these commands are actually successful like a transaction, the client can issue a waitflush 
-- command to check if the previous commands are successful. Please note that waitflush command 
-- may take up to 3 seconds or RaftSqliteStore.AutoFlushInterval to return. 
-- @param callbackFunc: function(err, fFlushed) end
function RaftSqliteStore:waitflush(query, callbackFunc, timeout)
	return self:InvokeCallback(callbackFunc, "NotImplemented", nil);
end

-- virtual:
-- this is usually used for changing database settings, such as cache size and sync mode. 
-- this function is specific to store implementation. 
-- @param query: string or {sql=string, CacheSize=number, IgnoreOSCrash=bool, IgnoreAppCrash=bool} 
function RaftSqliteStore:exec(query, callbackFunc)
	return self:InvokeCallback(callbackFunc, "NotImplemented", nil);
end

-- virtual:
-- this function never reply. the client will always timeout
function RaftSqliteStore:silient(query)
end

-- virtual: 
function RaftSqliteStore:makeEmpty(query, callbackFunc)
	self:removeIndex({}, function(err, bRemoved)
		local count = 0;
		self:delete({}, function(err, cnt) 
			count = cnt or 0;
		end)
		
		self:flush({}, function(err, bFlushed)
			if(not bFlushed) then
				LOG.std(nil, "warn", "makeEmpty", "failed to flush");
			end
			if(callbackFunc) then
				callbackFunc(nil, count);
			end
		end)
	end);
end
