--[[
Title: remote procedure call
Author(s): LiXizhi@yeah.net
Date: 2017.03.252017/2/8
Desc: Create RPC in current NPL thread. Internally it will use an existing or a virtual NPL activation file that can be invoked from any thread.
use the lib:
------------------------------------------------------------
NPL.load("(gl)script/ide/System/Concurrent/Rpc.lua");
local Rpc = commonlib.gettable("Raft.Rpc");
Rpc:new():init("Test.testRPC", function(self, msg) 
	LOG.std(nil, "info", "category", msg);
	msg.output=true; 
	ParaEngine.Sleep(1);
	return msg; 
end)

Test.testRPC:MakePublic();

-- now we can invoke it anywhere in any thread or remote address.
Test.testRPC("(worker1)", {"input"}, function(err, msg) 
	assert(msg.output == true and msg[1] == "input")
	echo(msg);
end);

-- time out in 500ms
Test.testRPC("(worker1)", {"input"}, function(err, msg) 
	assert(err == "timeout" and msg==nil)
	echo(err);
end, 500);
------------------------------------------------------------
]]

NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local RaftMessageType = NPL.load("(gl)script/Raft/RaftMessageType.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
local LoggerFactory = NPL.load("(gl)script/Raft/LoggerFactory.lua");

local logger = LoggerFactory.getLogger("Rpc")
local Rpc = commonlib.gettable("Raft.Rpc");

local rpc_instances = {};

function Rpc:new(o)
	o = o or {};
	o.logger = logger;
	o.run_callbacks = {};
	o.next_run_id = 0;
	o.thread_name = format("(%s)", __rts__:GetName());
	setmetatable(o, Rpc);
	return o;
end

-- @param funcName: global function name, such as "API.Auth"
-- @param handle_request_func: Rpc handler function of function(self, msg)  end
-- @param publicFileName: the activation file, if nil, it defaults to current file
function Rpc:init(funcName, handle_request_func, publicFileName)
	self:SetFuncName(funcName);
	self.handle_request = handle_request_func or echo;
	self:SetPublicFile(publicFileName);
	return self;
end

-- @param funcName: global function name, such as "API.Auth"
function Rpc:SetFuncName(funcName)
	self.fullname = funcName;
	self.filename = format("Rpc/%s.lua", self.fullname);
	if(commonlib.getfield(funcName)) then
		self.logger.warn("%s is overwritten", funcName);
	end
	commonlib.setfield(funcName, self);

	Rpc.AddInstance(funcName, self);
end

-- @param filename: the activation file, if nil, it defaults to current file
function Rpc:SetPublicFile(filename)
	filename = filename or format("Rpc/%s.lua", self.fullname);
	self.filename = filename;

	NPL.this(function() 
		self:OnActivated(msg);
	end, {filename = self.filename});

	self.logger.debug("%s installed to file %s", self.fullname, self.filename);
end

function Rpc:__tostring()
	return format("%s: (Rpc defined in %s)", self.fullname or "", self.filename or "");
end

-- static
function Rpc.AddInstance(name, o)
	if(name)then
		rpc_instances[name] = o;
	end
end

-- static
function Rpc.GetInstance(name)
	return rpc_instances[name or ""];
end

local added_runtime = {}
-- private: whenever a message arrives
function Rpc:OnActivated(msg)
	if type(self.localAddress) == "table" then
		self.logger.trace(msg)
	end
  if(msg.tid) then
     -- unauthenticated? reject as early as possible or accept it.
		 local messageType = msg.msg.messageType
     if(messageType) then
				local remoteAddress = msg.remoteAddress
				if messageType.int == RaftMessageType.ClientRequest.int then
					-- we got a client request
					if not added_runtime[remoteAddress.id] then
						added_runtime[remoteAddress.id] = true
						-- can not contain ':'
						local nid = string.sub(remoteAddress.id, 1, #remoteAddress.id-1);
						self.logger.trace("accepted nid is %s", nid)
					  NPL.AddNPLRuntimeAddress({host = remoteAddress.host, port = remoteAddress.port, nid = nid})
					end
					NPL.accept(msg.tid, remoteAddress.id or "default_user");
					msg.nid = remoteAddress.id or "default_user"
        else
					-- body
				  NPL.accept(msg.tid, remoteAddress or "default_user");
				  msg.nid = remoteAddress or "default_user"
				end
     else
        NPL.reject(msg.tid);
     end
	end
	
  if(msg.nid) then
    -- only respond to authenticated messages. 
		if(msg.name) then
			local rpc_ = Rpc.GetInstance(msg.name);
			if(type(rpc_) == "table" and rpc_.OnActivated) then
				msg.name = nil;
				return rpc_:OnActivated(msg);
			end
		end
		
		if(msg.type=="run") then
			local result, err = self:handle_request(msg.msg);
			if not result then
				return
			end
			if type(msg.remoteAddress) == "table" and msg.remoteAddress.id then
				msg.remoteAddress = msg.remoteAddress.id
			end
			local vFileId = format("%s%s%s", msg.callbackThread, msg.remoteAddress, self.filename)
			local response = {
				name = self.fullname,
				type="result",
				msg = result, 
				err=err,
				remoteAddress = self.localAddress, -- on the server side the local address is nil
				callbackId = msg.callbackId
			}
			if type(self.localAddress) == "table" then
				self.logger.debug("activate on %s, msg:%s", vFileId, util.table_tostring(response))
			end
			local activate_result = NPL.activate(vFileId, response)

			-- handle memory leak
			if activate_result ~= 0 then
				-- FIXME:
				-- this will cause remote side memory leak, how to handle this
				-- should give run_callbacks a TTL
				-- self.run_callbacks[callbackId] = nil
				self.logger.error("activate on %s failed %d", vFileId, activate_result)
			end

		elseif(msg.type== "result" and msg.callbackId) then
			self:InvokeCallback(msg.callbackId, msg.err, msg.msg);
		end
	end
end

-- private invoke callback and remove from queue, 
function Rpc:InvokeCallback(callbackId, err, msg)
	local callback = self.run_callbacks[callbackId];
	if(callback) then
		if(callback.timer) then
			callback.timer:Change();
		end
		self.run_callbacks[callbackId] = nil;
		if(type(callback.callbackFunc) == "function") then
			callback.callbackFunc(err, msg);
		end
	end
end

-- smallest short value to avoid conflicts with manual id. 
local min_short_value = 100000;

-- by default, Rpc can only be called from threads in the current process. 
-- in order to expose the API via NPL tcp protocol, one needs to call this function. 
function Rpc:MakePublic()
	NPL.load("(gl)script/ide/System/Encoding/crc32.lua");
	local Encoding = commonlib.gettable("System.Encoding");
	local shortValue = Encoding.crc32(self.filename);
	if(shortValue < min_short_value) then
		shortValue = shortValue + min_short_value;
	end
	NPL.AddPublicFile(self.filename, shortValue);
end

-- @param address: if nil, it is current NPL thread. it can be thread name like "(worker1)"
-- if NPL thread worker1 is not created, it will be automatically created. 
-- Because NPL thread is reused, it is good practice to use only limited number of NPL threads per process.
-- for complete format, please see NPL.activate function. 
-- @param msg: any table object
-- @param callbackFunc: result from the Rpc, function(err, msg) end
-- @param timeout:  time out in milliseconds. if nil, there is no timeout
-- if timed out callbackFunc("timeout", nil) is invoked on timeout
function Rpc:activate(localAddress, remoteAddress, msg, callbackFunc, timeout)
	-- if(type(address) == "string") then
	-- 	local thread_name = address:match("^%((.+)%)$");
	-- 	if (thread_name) then
	-- 		self.workers = self.workers or {};
	-- 		if(not self.workers[address]) then
	-- 			self.workers[address] = true;
	-- 			NPL.CreateRuntimeState(thread_name, 0):Start();
	-- 		end
	-- 	end
	-- end
	-- thread_name = thread_name or "osAsync";
	-- NPL.CreateRuntimeState(thread_name, 0):Start();

	self.localAddress = localAddress
	self.remoteAddress = remoteAddress

	local callbackId = self.next_run_id + 1;
	self.next_run_id = callbackId
	local callback = {
		callbackFunc = callbackFunc,
		timeout = timeout,
	};
	self.run_callbacks[callbackId] = callback;
	if(timeout and timeout>0) then
		callback.timer = callback.timer or commonlib.Timer:new({callbackFunc = function(timer)
			self:InvokeCallback(callbackId, "timeout", nil);
		end});
		callback.timer:Change(timeout, nil)
	end


	local vFileId = format("%s%s", self.remoteAddress or "", self.filename)
	local msg = {
		type="run", 
		msg = msg, 
		name = self.fullname,
		callbackId = self.next_run_id, 
		callbackThread = self.thread_name,
		remoteAddress = self.localAddress,
	}
	if type(self.localAddress) == "table" then
		self.logger.debug("activate on %s, msg:%s", vFileId, util.table_tostring(msg))
	end
	local activate_result = NPL.activate(vFileId, msg);
	-- handle memory leak
	if activate_result ~= 0 then
		self.run_callbacks[callbackId] = nil
		self.logger.error("activate on %s failed %d", vFileId, activate_result)
	else
		-- FIXME:
		-- to avoid memory leak, we 
		-- should give self.run_callbacks a TTL
		-- self.run_callbacks[callbackId] = nil
	end
	return activate_result
end

Rpc.__call = Rpc.activate;
Rpc.__index = Rpc;