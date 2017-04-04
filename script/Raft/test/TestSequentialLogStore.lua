--[[
Title: 
Author: liuluheng
Date: 2017.04.03
Desc: 
TEST SequentialLogStore
------------------------------------------------------------
NPL.load("(gl)script/ide/UnitTest/luaunit.lua");
NPL.load("(gl)script/Raft/test/TestSequentialLogStore.lua");
LuaUnit:run('TestSequentialLogStore') 
------------------------------------------------------------
]]--

NPL.load("(gl)script/ide/Files.lua");
NPL.load("(gl)script/Raft/LogEntry.lua");
local LogEntry = commonlib.gettable("Raft.LogEntry");
NPL.load("(gl)script/Raft/ClusterConfiguration.lua");
local ClusterConfiguration = commonlib.gettable("Raft.ClusterConfiguration");
NPL.load("(gl)script/Raft/Snapshot.lua");
local Snapshot = commonlib.gettable("Raft.Snapshot");
NPL.load("(gl)script/Raft/SequentialLogStore.lua");
local SequentialLogStore = commonlib.gettable("Raft.SequentialLogStore");
NPL.load("(gl)script/Raft/ClusterServer.lua");
local ClusterServer = commonlib.gettable("Raft.ClusterServer");
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")

NPL.load("(gl)script/Raft/test/TestUtil.lua");
NPL.load("(gl)script/ide/UnitTest/luaunit.lua");


local MAX_LONG = 2^63 - 1;
local MAX_INT = 2^31 - 1;

local removeTestFiles, randomLogEntry, logEntriesEquals;

local assertTrue = assert

TestSequentialLogStore = {}

function TestSequentialLogStore:testBuffer()
    local container = "temp/snapshot/";
    -- commonlib.Files.TouchFolder(container); -- this not works
    ParaIO.CreateDirectory(container);
    local store = SequentialLogStore:new(container);
    local logsCount = math.random(1000) + 1500;
    local entries = {};
    for i = 1, logsCount do
        local entry = randomLogEntry();
        store:append(entry);
        entries[#entries + 1] = entry;
    end

    local start = math.random(logsCount - 1000);
    local endi = logsCount - 500;
    local results = store:getLogEntries(start, endi);
    assertEquals(#results, endi - start)
    for i = start, endi - 1 do
        logEntriesEquals(entries[i], results[i - start + 1]);
    end

    store:close();

    removeTestFiles(container);
end


function TestSequentialLogStore:testStore()
    local container = "temp/snapshot/";
    removeTestFiles(container);
    ParaIO.CreateDirectory(container);
    local store = SequentialLogStore:new(container);
    assertTrue(store:getLastLogEntry().term == 0);
    assertTrue(store:getLastLogEntry().value == nil);
    assertEquals(1, store:getFirstAvailableIndex());
    assertTrue(store:getLogEntryAt(1) == nil);

    -- write some logs
    local entries = {};
    for i = 1, math.random(100) + 10 do
        local entry = randomLogEntry();
        store:append(entry);
        entries[#entries + 1] = entry;
    end


    assertEquals(#entries, store:getFirstAvailableIndex() - 1);
    assertTrue(logEntriesEquals(entries[#entries], store:getLastLogEntry()));

    -- random item
    local randomIndex = math.random(#entries);
    assertTrue(logEntriesEquals(entries[randomIndex], store:getLogEntryAt(randomIndex))); -- log store's index starts from 1

    -- random range
    randomIndex = math.random(#entries);
    local randomSize = math.random(#entries - randomIndex);
    local logEntries = store:getLogEntries(randomIndex, randomIndex + randomSize);

    for i= randomIndex, randomIndex + randomSize - 1 do
        assertTrue(logEntriesEquals(entries[i], logEntries[i - randomIndex + 1]));
    end


    store:close();
    store = SequentialLogStore:new(container);

    assertEquals(#entries, store:getFirstAvailableIndex() - 1);
    assertTrue(logEntriesEquals(entries[#entries], store:getLastLogEntry()));

    -- random item
    randomIndex = math.random(#entries);
    assertTrue(logEntriesEquals(entries[randomIndex], store:getLogEntryAt(randomIndex))); -- log store's index starts from 1

    -- random range
    randomIndex = math.random(#entries);
    randomSize = math.random(#entries - randomIndex);
    logEntries = store:getLogEntries(randomIndex, randomIndex + randomSize);

    for i= randomIndex, randomIndex + randomSize - 1 do
        assertTrue(logEntriesEquals(entries[i], logEntries[i - randomIndex + 1]));
    end

    -- test with edge
    randomSize = math.random(#entries);
    logEntries = store:getLogEntries(store:getFirstAvailableIndex() - randomSize, store:getFirstAvailableIndex());

    local j = 1
    for i= #entries - randomSize + 1, #entries do
        assertTrue(logEntriesEquals(entries[i], logEntries[j]));
        j = j + 1;
    end

    -- test write at
    local logEntry = randomLogEntry();
    randomIndex = math.random(store:getFirstAvailableIndex() - 1);
    store:writeAt(store:getStartIndex() + randomIndex, logEntry);
    assertEquals(randomIndex + store:getStartIndex(), store:getFirstAvailableIndex());
    assertTrue(logEntriesEquals(logEntry, store:getLastLogEntry()));

    store:close();
    removeTestFiles(container);

end



function removeTestFiles(container)
    commonlib.Files.DeleteFolder(container);
end

function randomLogEntry()
    local term = math.random(MAX_LONG);
    local value = string.random(math.random( 20 ), "%l%d")
    local type = math.random(5) - 1;
    return LogEntry:new(term, value, type);
end

function logEntriesEquals(entry1, entry2)
    local equals = entry1.term == entry2.term and entry1.valueType == entry2.valueType;
    -- util.table_print(entry1)
    -- util.table_print(entry2)
    equals = equals and ((entry1.value ~= nil and entry2.value ~= nil and #entry1.value == #entry2.value) or (entry1.value == nil and entry2.value == nil));
    if(entry1.value ~= nil) then
        local i = 1;
        while(equals and i < #entry1.value) do
            equals = string.byte(entry1.value, i) == string.byte(entry2.value, i);
            i = i + 1;
        end
    end

    assert(equals);

    return equals;
end