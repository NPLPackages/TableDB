--[[
Title:
Author: liuluheng
Date: 2017.04.12
Desc:
TEST

------------------------------------------------------------
NPL.load("(gl)script/ide/UnitTest/luaunit.lua");
NPL.load("(gl)npl_mod/TableDB/test/TestSqlHandler.lua");
LuaUnit:run('TestSqlHandler')
------------------------------------------------------------
]]
--
NPL.load("(gl)script/ide/commonlib.lua");
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)npl_mod/TableDB/SQLHandler.lua");
local SQLHandler = commonlib.gettable("TableDB.SQLHandler");
NPL.load("(gl)npl_mod/TableDB/RaftLogEntryValue.lua");
local RaftLogEntryValue = commonlib.gettable("TableDB.RaftLogEntryValue");
NPL.load("(gl)script/ide/UnitTest/luaunit.lua");

local thread_name = format("(%s)", __rts__:GetName());
local removeTestFiles

TestSqlHandler = {}

function TestSqlHandler:testPerformance()
    local rootFolder = "temp/database/"
    local baseDir = "temp/testSqlHandler/"
    
    removeTestFiles(rootFolder)
    removeTestFiles(baseDir)
    
    local enableSyncMode = false;
    local serverId = "server0:"
    local sqlHandler = SQLHandler:new(baseDir, true);
    sqlHandler:start();
    
    -- connect
    local query_type = "connect"
    local collection = {
        ToData = function(...) end,
    }
    local query = {
        rootFolder = rootFolder,
    }
    local raftLogEntryValue = RaftLogEntryValue:new(query_type, collection, query, -1, serverId, enableSyncMode, thread_name)
    local bytes = raftLogEntryValue:toBytes();
    sqlHandler:handle(bytes);
    
    
    NPL.load("(gl)script/ide/Debugger/NPLProfiler.lua");
    local npl_profiler = commonlib.gettable("commonlib.npl_profiler");
    npl_profiler.perf_reset();
    
    npl_profiler.perf_begin("testPerformance", true)
    local total_times = 10000; -- a million non-indexed insert operation
    local max_jobs = 1000; -- concurrent jobs count
    NPL.load("(gl)script/ide/System/Concurrent/Parallel.lua");
    local Parallel = commonlib.gettable("System.Concurrent.Parallel");
    local p = Parallel:new():init()
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
        
        sqlHandler:handle(bytes, function(err, data)
            if (err) then
                echo({err, data});
            end
            local msg = {
                err = err,
                data = data,
                cb_index = raftLogEntryValue.cb_index,
            }
    
            -- log(format("Result:%s\n", util.table_tostring(msg)))

            local remoteAddress = format("%s%s", raftLogEntryValue.callbackThread, raftLogEntryValue.serverId)
            
            RTDBRequestRPC(nil, remoteAddress, msg);
            p:Next();
        end)
    end, total_times, max_jobs):OnFinished(function(total)
        npl_profiler.perf_end("testPerformance", true)
        log(commonlib.serialize(npl_profiler.perf_get(), true));
    end);
end

function removeTestFiles(container)
    commonlib.Files.DeleteFolder(container);
end
