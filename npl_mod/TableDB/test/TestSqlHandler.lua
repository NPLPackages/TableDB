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
    local baseDir = "temp/testSqlHandler"

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
    
    for i = 1, 10000 do
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
        
        sqlHandler:handle(bytes);
    end
end

function removeTestFiles(container)
    commonlib.Files.DeleteFolder(container);
end
