--[[
Title: 
Author: 
Date: 
Desc: 
]]--

NPL.load("(gl)script/ide/commonlib.lua");
NPL.load("(gl)script/Raft/ServerState.lua");
local ServerState = commonlib.gettable("Raft.ServerState");


local serverState = ServerState:new()

print(serverState:tostring())
serverState:increaseTerm()
print(serverState:tostring())



--[[
local function activate()
   if(msg) then
      print(msg.data or "");
      --- C/C++ API call is counted as one instruction, so if you call ParaEngine.Sleep(10), 
      --it will block all concurrent jobs on that NPL thread for 10 seconds
      ParaEngine.Sleep(0.5);
   end
   NPL.activate("(gl)script/helloworld.lua", {data="hello world!"})
end

NPL.this(activate);
]]--