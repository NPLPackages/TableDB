--[[
Title:
Author: liuluheng
Date: 2017.04.12
Desc:
TEST

------------------------------------------------------------
NPL.load("(gl)script/ide/UnitTest/luaunit.lua");
NPL.load("(gl)npl_mod/TableDB/test/TestRaftLogEntryValue.lua");
LuaUnit:run('TestRaftLogEntryValue')
------------------------------------------------------------
]]
--
NPL.load("(gl)script/ide/commonlib.lua");
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)npl_mod/TableDB/RaftLogEntryValue.lua");
local RaftLogEntryValue = commonlib.gettable("TableDB.RaftLogEntryValue");

NPL.load("(gl)script/ide/UnitTest/luaunit.lua");

local thread_name = format("(%s)", __rts__:GetName());

local MAX_LONG = 2 ^ 63 - 1;
local MAX_INT = 2 ^ 31 - 1;

TestRaftLogEntryValue = {}

function TestRaftLogEntryValue:testSerialization()
    local query_type = "find"
    local collection = {
        name = "test",
        db = "temp/database",
        ToData = function(...)
            return {name = "test",
                db = "temp/database", }
        end
    }
    local query = {
        filed = "test",
        update = "no",
    }
    
    local raftLogEntryValue = RaftLogEntryValue:new(query_type, collection, query);
    local bytes = raftLogEntryValue:toBytes();
    local raftLogEntryValue2 = RaftLogEntryValue:fromBytes(bytes);
    
    -- util.table_print(raftLogEntryValue)
    -- util.table_print(raftLogEntryValue2)
    assert(commonlib.partialcompare(raftLogEntryValue, raftLogEntryValue2))
    
    
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
    
    assert(commonlib.partialcompare(raftLogEntryValue, raftLogEntryValue2))
end

function TestRaftLogEntryValue:testPerformance()
    local enableSyncMode = false;
    local serverId = "server0:"
    NPL.load("(gl)script/ide/Debugger/NPLProfiler.lua");
    local npl_profiler = commonlib.gettable("commonlib.npl_profiler");
    npl_profiler.perf_reset();
    
    npl_profiler.perf_begin("testPerformance", true)
    local total_times = 10000; -- a million non-indexed insert operation
    local max_jobs = 1000; -- concurrent jobs count
    NPL.load("(gl)script/ide/System/Concurrent/Parallel.lua");
    local Parallel = commonlib.gettable("System.Concurrent.Parallel");
    local p = Parallel:new():init()
    p:OnFinished(function(total)
        npl_profiler.perf_end("testPerformance", true)
        log(commonlib.serialize(npl_profiler.perf_get(), true));
    end);
    
    p:RunManyTimes(function(count)
        local i = count
        local query_type = "insertOne"
        local collection = {
            name = "testPerformance",
            db = rootFolder,
            ToData = function(...)
                return {name = "testPerformance",
                    db = rootFolder, }
            end
        }
        local query = {
            field1 = "test" .. i,
            fieldno = "no" .. i,
        }
        
        local raftLogEntryValue = RaftLogEntryValue:new(query_type, collection, query, i, serverId, enableSyncMode, thread_name);
        local bytes = raftLogEntryValue:toBytes();
        
        local ignore = RaftLogEntryValue:fromBytes(bytes);
        p:Next();
    
    end, total_times, max_jobs)

end
