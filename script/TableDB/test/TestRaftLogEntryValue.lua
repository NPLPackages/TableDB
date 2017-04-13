--[[
Title: 
Author: liuluheng
Date: 2017.04.12
Desc: 
TEST

------------------------------------------------------------
NPL.load("(gl)script/ide/UnitTest/luaunit.lua");
NPL.load("(gl)script/TableDB/test/TestRaftLogEntryValue.lua");
LuaUnit:run('TestRaftLogEntryValue') 
------------------------------------------------------------
]]--

NPL.load("(gl)script/ide/commonlib.lua");
NPL.load("(gl)script/TableDB/RaftLogEntryValue.lua");
local RaftLogEntryValue = commonlib.gettable("TableDB.RaftLogEntryValue");

NPL.load("(gl)script/ide/UnitTest/luaunit.lua");


local MAX_LONG = 2^63 - 1;
local MAX_INT = 2^31 - 1;

TestRaftLogEntryValue = {}

function TestRaftLogEntryValue:testSerialization()
  local query_type = "find"
  local collection = {
    name = "test",
    db = "temp/database",
  }
  local query = {
    filed = "test",
    update = "no",
  }

  local raftLogEntryValue = RaftLogEntryValue:new(query_type, collection, query);
  local bytes = raftLogEntryValue:toBytes();
  local raftLogEntryValue2 = RaftLogEntryValue:fromBytes(bytes);

  assert(commonlib.compare(raftLogEntryValue, raftLogEntryValue2))


  local query_type = "update"
  query = {
    query = "query",
    update = {
      f1 = "2",
      f2 = 3,
      f3 = "kks"
    },
  }

  local raftLogEntryValue = RaftLogEntryValue:new(query_type, collection, query);
  local bytes = raftLogEntryValue:toBytes();
  local raftLogEntryValue2 = RaftLogEntryValue:fromBytes(bytes);

  assert(commonlib.compare(raftLogEntryValue, raftLogEntryValue2))


end