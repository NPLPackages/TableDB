--[[
Title: 
Author: 
Date: 
Desc: 
]]--

NPL.load("(gl)script/ide/commonlib.lua");
NPL.load("(gl)script/Raft/ServerState.lua");

local ServerState = commonlib.gettable("Raft.ServerState");
local ServerRole = NPL.load("(gl)script/Raft/ServerRole.lua");
logger = commonlib.logging.GetLogger("")

local serverState = ServerState:new()

logger.info(serverState)

-- must be colon, to provide the hidden self
serverState:increaseTerm()
logger.info(serverState)

logger.info(ServerRole.Follower)


test = {
  -- a = 1,
  -- b = "tes",
  -- c = 2.0
}
test = nil

t2 = {
  a2 = 2,
  b2 = "3f"
}

for k,v in pairs(test) do
  t2[k] = v

end

for k,v in pairs(t2) do
  print(k,v)
end

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