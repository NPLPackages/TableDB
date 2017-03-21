--[[
Title: 
Author: 
Date: 
Desc: 
]]--

NPL.load("(gl)script/ide/commonlib.lua");
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
NPL.load("(gl)script/Raft/ServerState.lua");

-- local ServerState = commonlib.gettable("Raft.ServerState");
local ServerRole = NPL.load("(gl)script/Raft/ServerRole.lua");
local util = commonlib.gettable("System.Compiler.lib.util")

NPL.load("(gl)script/Raft/PeerServer.lua");
local PeerServer = commonlib.gettable("Raft.PeerServer");

logger = commonlib.logging.GetLogger("")

local RaftMessageType = NPL.load("(gl)script/Raft/RaftMessageType.lua");

-- assert(false,"assert is working")

-- local o = {
--   t = RaftMessageType.RequestVoteRequest,
-- }
-- logger.debug(o)
-- logger.debug(o.t == RaftMessageType.RequestVoteRequest )

-- local serverState = ServerState:new()


-- NPL.load("(gl)script/ide/timer.lua");

-- local mytimer = commonlib.Timer:new({callbackFunc = function(timer)
-- 	logger({"ontimer", timer.id, timer.delta, timer.lastTick})
--   mytimer:Change()
-- end})

-- -- start the timer after 0 milliseconds, and signal every 1000 millisecond
-- logger.error("here")
-- mytimer:Change(0, 1000)
-- -- ParaEngine.Sleep(2);
-- -- 
-- -- mytimer:Change()
-- -- ParaEngine.Sleep(1);
-- logger.error("here2")
-- mytimer:Change(0, 1000)
-- logger.error("here")


-- logger.info(serverState)

-- -- must be colon, to provide the hidden self
-- serverState:increaseTerm()
-- logger.info(serverState)

logger.info(ServerRole.Follower)




-- local t = {
--   id = 1,

--   handler = function () 
--     print("kkkk")
--   end,
  
-- }

-- local q = {
--   c = function ()

--     print("c")
--   end,
--   ts = {}
-- }

-- t.heartbeatTimer = commonlib.Timer:new({callbackFunc = function(timer)
--                                             q.c()

--                                            end})
-- util.table_print(t.heartbeatTimer)

local q = {
  ts = {}
}

-- table.insert( q.ts,t.id,t )

-- util.table_print(q.ts)

-- t.heartbeatTimer:Change(2, nil)


for i=1,2 do
  local p = PeerServer:new(nil,nil,nil)
  q.ts[i] = p
end

util.table_print(q)


-- test = {
--   a = "k",
--   b ="l"
-- }

-- util.table_print(test)

-- NPL.load("(gl)script/Raft/rpc.lua");
-- local rpc = commonlib.gettable("System.Concurrent.Async.rpc");
-- rpc:new():init("Test.testRPC", function(self, msg) 
-- 	LOG.std(nil, "info", "category", msg);
-- 	msg.output=true; 
-- 	-- ParaEngine.Sleep(1);
-- 	return msg; 
-- end)

-- NPL.StartNetServer("127.0.0.1", "60001");
-- NPL.AddNPLRuntimeAddress({host = "127.0.0.1", port = "60002", nid = "server2"})
-- Test.testRPC:MakePublic();
-- print(Test.testRPC)

-- -- now we can invoke it anywhere in any thread or remote address.
-- while(Test.testRPC("server1:","server2:", {"input"}, function(err, msg) 
--    LOG.std(nil, "info", "category", msg);
-- 	assert(msg.output == true and msg[1] == "input")
-- end) ~= 0) do end;

-- -- time out in 500ms
-- Test.testRPC("(worker1)", {"input"}, function(err, msg) 
-- 	assert(err == "timeout" and msg==nil)
-- 	echo(err);
-- end, 500);

-- NPL.activate("rpc/Test.testRPC.lua",{
-- 		type="run", 
-- 		msg = {"imputtest"}, 
-- 		name = "Test.testRPC",
-- 		-- callbackId = self.next_run_id, 
-- 		callbackThread="(osAsync)",
-- 	})


-- NPL.load("(gl)script/test/network/TestSimpleServer.lua");
-- test_start_simple_server();

--[[
local function activate()
   if(msg) then
      logger.info(msg.data or "");
      --- C/C++ API call is counted as one instruction, so if you call ParaEngine.Sleep(10), 
      --it will block all concurrent jobs on that NPL thread for 10 seconds
      ParaEngine.Sleep(0.5);
   end
   NPL.activate("(gl)script/helloworld.lua", {data="hello world!"})
end

NPL.this(activate);
]]--