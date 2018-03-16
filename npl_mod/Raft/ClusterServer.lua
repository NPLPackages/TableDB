--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 

Cluster server configuration 
a class to hold the configuration information for a server in a cluster
------------------------------------------------------------
NPL.load("(gl)npl_mod/Raft/ClusterServer.lua");
local ClusterServer = commonlib.gettable("Raft.ClusterServer");
------------------------------------------------------------
]] --
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua")
local util = commonlib.gettable("System.Compiler.lib.util")

local ClusterServer = commonlib.gettable("Raft.ClusterServer")

function ClusterServer:new(bytesOrTable)
  local o = {
    id = 0,
    endpoint = nil
  }
  if bytesOrTable then
    if type(bytesOrTable) == "table" then
      local server = bytesOrTable
      o.id = server.id
      o.endpoint = server.endpoint
    elseif type(bytesOrTable) == "string" then
      local bytes = bytesOrTable
      local file = ParaIO.open("<memory>", "w")
      if (file:IsValid()) then
        -- can not use file:WriteString(bytes);, use WriteBytes
        -- file:WriteBytes(#bytes, {bytes:byte(1, -1)});
        file:write(bytes, #bytes)
        file:seek(0)
        o.id = file:ReadInt()
        local endpointLength = file:ReadInt()
        o.endpoint = file:ReadString(endpointLength)
        file:close()
      end
    end
  end

  setmetatable(o, self)
  return o
end

function ClusterServer:__index(name)
  return rawget(self, name) or ClusterServer[name]
end

function ClusterServer:__tostring()
  return util.table_tostring(self)
end
 --

--[[
  Serialize a server configuration to binary data
  @return the binary data that represents the server configuration
]] function ClusterServer:toBytes()
  -- "<memory>" is a special name for memory file, both read/write is possible.
  local file = ParaIO.open("<memory>", "w")
  local bytes
  if (file:IsValid()) then
    file:WriteInt(self.id)
    file:WriteInt(#self.endpoint)
    file:WriteString(self.endpoint)

    bytes = file:GetText(0, -1)

    file:close()
  end
  return bytes
end
