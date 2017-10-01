--[[
Title:
Author: liuluheng
Date: 2017.04.03
Desc:
TEST WALSequentialLogStore
------------------------------------------------------------
NPL.load("(gl)script/ide/UnitTest/luaunit.lua");
NPL.load("(gl)npl_mod/Raft/test/TestWALSequentialLogStore.lua");
LuaUnit:run('TestWALSequentialLogStore')
------------------------------------------------------------
]]
--
NPL.load("(gl)script/ide/Files.lua");
NPL.load("(gl)npl_mod/Raft/LogEntry.lua");
local LogEntry = commonlib.gettable("Raft.LogEntry");
NPL.load("(gl)npl_mod/Raft/WALSequentialLogStore.lua");
local WALSequentialLogStore = commonlib.gettable("Raft.WALSequentialLogStore");
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")

NPL.load("(gl)npl_mod/Raft/test/TestUtil.lua");
NPL.load("(gl)script/ide/UnitTest/luaunit.lua");


local MAX_LONG = 2 ^ 13 - 1;
local MAX_INT = 2 ^ 31 - 1;

local removeTestFiles, randomLogEntry, logEntriesEquals;

local assertTrue = assert

TestWALSequentialLogStore = {}

function TestWALSequentialLogStore:testBuffer()
    local container = "temp/snapshot/";
    -- commonlib.Files.TouchFolder(container); -- this not works
    removeTestFiles(container);
    ParaIO.CreateDirectory(container);
    local store = WALSequentialLogStore:new(container);
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

function TestWALSequentialLogStore:testPackAndUnpack()
    local container = "temp/snapshot/";
    removeTestFiles(container);
    ParaIO.CreateDirectory(container);
    local container1 = "temp/snapshot1/";
    removeTestFiles(container1);
    ParaIO.CreateDirectory(container1);
    local store = WALSequentialLogStore:new(container);
    local store1 = WALSequentialLogStore:new(container1);
    
    -- write some logs
    local logsCount = math.random(1000) + 1000;
    for i = 1, logsCount do
        local entry = randomLogEntry();
        store:append(entry);
        store1:append(entry);
    end
    
    local logsCopied = 0;
    while (logsCopied < logsCount) do
        local pack = store:packLog(logsCopied + 1, 100);
        store1:applyLogPack(logsCopied + 1, pack);
        logsCopied = math.min(logsCopied + 100, logsCount);
    end
    
    assertEquals(store:getFirstAvailableIndex(), store1:getFirstAvailableIndex());
    for i = 1, logsCount do
        local entry1 = store:getLogEntryAt(i);
        local entry2 = store1:getLogEntryAt(i);
        assertTrue(logEntriesEquals(entry1, entry2),
            format("the " .. i .. "th value are not equal(total: " .. logsCount .. ")\n\tentry1:%s,\n\tentry2:%s",
                util.table_tostring(entry1), util.table_tostring(entry2)));
    end
    

    -- test append after applyLogPack
    -- write some logs
    local entries = {};
    for i = 1, math.random(100) + 10 do
        local entry = randomLogEntry();
        store1:append(entry);
        entries[#entries + 1] = entry;
    end
    
    assertEquals(#entries + logsCount, store1:getFirstAvailableIndex() - 1);
    -- print(util.table_tostring(entries[#entries]), util.table_tostring(store1:getLastLogEntry()))
    assertTrue(logEntriesEquals(entries[#entries], store1:getLastLogEntry()));
    assertTrue(logEntriesEquals(entries[#entries], store1:getLogEntryAt(#entries + logsCount)));
    
    -- random item
    local randomIndex = math.random(#entries);
    assertTrue(logEntriesEquals(entries[randomIndex], store1:getLogEntryAt(randomIndex + logsCount))); -- log store's index starts from 1
    
    -- random range
    randomIndex = math.random(#entries);
    local randomSize = math.random(#entries - randomIndex);
    local logEntries = store1:getLogEntries(randomIndex, randomIndex + logsCount + randomSize);
    
    for i = randomIndex, randomIndex + randomSize - 1 do
        assertTrue(logEntriesEquals(entries[i], logEntries[i - randomIndex + 1 + logsCount]));
    end
    
    
    -- test write at
    local logEntry = randomLogEntry();
    randomIndex = math.random(store1:getFirstAvailableIndex() - 1);
    store1:writeAt(store1:getStartIndex() + randomIndex, logEntry);
    assertEquals(randomIndex + store1:getStartIndex(), store1:getFirstAvailableIndex());
    assertTrue(logEntriesEquals(logEntry, store1:getLastLogEntry()));
    
    store:close();
    store1:close();
    removeTestFiles(container);
    removeTestFiles(container1);
end


function TestWALSequentialLogStore:testStore()
    local container = "temp/snapshot/";
    removeTestFiles(container);
    ParaIO.CreateDirectory(container);
    local store = WALSequentialLogStore:new(container);
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
    
    for i = randomIndex, randomIndex + randomSize - 1 do
        assertTrue(logEntriesEquals(entries[i], logEntries[i - randomIndex + 1]));
    end
    
    
    -- WAL new not read logs
    -- store:close();
    -- store = WALSequentialLogStore:new(container);
    assertEquals(#entries, store:getFirstAvailableIndex() - 1);
    assertTrue(logEntriesEquals(entries[#entries], store:getLastLogEntry()));
    
    -- random item
    randomIndex = math.random(#entries);
    assertTrue(logEntriesEquals(entries[randomIndex], store:getLogEntryAt(randomIndex))); -- log store's index starts from 1
    
    -- random range
    randomIndex = math.random(#entries);
    randomSize = math.random(#entries - randomIndex);
    logEntries = store:getLogEntries(randomIndex, randomIndex + randomSize);
    
    for i = randomIndex, randomIndex + randomSize - 1 do
        assertTrue(logEntriesEquals(entries[i], logEntries[i - randomIndex + 1]));
    end
    
    -- test with edge
    randomSize = math.random(#entries);
    logEntries = store:getLogEntries(store:getFirstAvailableIndex() - randomSize, store:getFirstAvailableIndex());
    
    local j = 1
    for i = #entries - randomSize + 1, #entries do
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

function TestWALSequentialLogStore:testCompactRandom()
    local container = "temp/snapshot/";
    -- commonlib.Files.TouchFolder(container); -- this not works
    removeTestFiles(container);
    ParaIO.CreateDirectory(container);
    local store = WALSequentialLogStore:new(container);
    local logsCount = 300;
    local entries = {};
    for i = 1, logsCount do
        local entry = randomLogEntry();
        store:append(entry);
        entries[#entries + 1] = entry;
    end
    
    local lastLogIndex = #entries;
    local indexToCompact = math.random(lastLogIndex - 10) + 1;
    store:compact(indexToCompact);
    
    assertEquals(indexToCompact + 1, store:getStartIndex());
    assertEquals(#entries, store:getFirstAvailableIndex() - 1);
    
    for i = 1, store:getFirstAvailableIndex() - indexToCompact - 1 do
        local entry = store:getLogEntryAt(store:getStartIndex() + i - 1);
        assertTrue(logEntriesEquals(entries[i + indexToCompact], entry));
    end
    
    local randomIndex = math.random(store:getFirstAvailableIndex() - indexToCompact - 1);
    local logEntry = randomLogEntry();
    store:writeAt(store:getStartIndex() + randomIndex, logEntry);
    entries[randomIndex + indexToCompact + 1] = logEntry;
    
    for i = randomIndex + indexToCompact + 2, #entries do
        entries[i] = nil;
    end
    
    for i = 1, store:getFirstAvailableIndex() - indexToCompact - 1 do
        local entry = store:getLogEntryAt(store:getStartIndex() + i - 1);
        assertTrue(logEntriesEquals(entries[i + indexToCompact], entry));
    end
    
    for i = 1, math.random(100) + 10 do
        local entry = randomLogEntry();
        entries[#entries + 1] = entry;
        store:append(entry);
    end
    
    for i = 1, store:getFirstAvailableIndex() - indexToCompact - 1 do
        local entry = store:getLogEntryAt(store:getStartIndex() + i - 1);
        assertTrue(logEntriesEquals(entries[i + indexToCompact], entry));
    end
    
    
    store:close();
    removeTestFiles(container);

end

function TestWALSequentialLogStore:testCompactAll()
    local container = "temp/snapshot/";
    removeTestFiles(container);
    ParaIO.CreateDirectory(container);
    local store = WALSequentialLogStore:new(container);
    
    -- write some logs
    local entries = {};
    for i = 1, math.random(1000) + 10 do
        local entry = randomLogEntry();
        entries[#entries + 1] = entry;
        store:append(entry);
    end
    
    assertEquals(1, store:getStartIndex());
    assertEquals(#entries, store:getFirstAvailableIndex() - 1);
    assertTrue(logEntriesEquals(entries[#entries], store:getLastLogEntry()));
    local lastLogIndex = #entries;
    store:compact(lastLogIndex);
    
    assertEquals(#entries + 1, store:getStartIndex());
    assertEquals(#entries, store:getFirstAvailableIndex() - 1);
    
    for i = 1, math.random(100) + 10 do
        local entry = randomLogEntry();
        entries[#entries + 1] = entry;
        store:append(entry);
    end
    
    assertEquals(lastLogIndex + 1, store:getStartIndex());
    assertEquals(#entries, store:getFirstAvailableIndex() - 1);
    assertTrue(logEntriesEquals(entries[#entries], store:getLastLogEntry()));
    
    local index = store:getStartIndex() + math.random(store:getFirstAvailableIndex() - store:getStartIndex());
    assertTrue(logEntriesEquals(entries[index], store:getLogEntryAt(index)));
    
    store:close();
    removeTestFiles(container);
end


function removeTestFiles(container)
    commonlib.Files.DeleteFolder(container);
end

function randomLogEntry()
    local term = math.random(MAX_LONG);
    local value = string.random(math.random(4096), "%l%d")
    local type = math.random(5) - 1;
    return LogEntry:new(term, value, type);
end

function logEntriesEquals(entry1, entry2)
    local equals = entry1.term == entry2.term and entry1.valueType == entry2.valueType;
    
    equals = equals and ((entry1.value ~= nil and entry2.value ~= nil and #entry1.value == #entry2.value) or (entry1.value == nil and entry2.value == nil));
    if (entry1.value ~= nil) then
        local i = 1;
        while (equals and i < #entry1.value) do
            equals = string.byte(entry1.value, i) == string.byte(entry2.value, i);
            i = i + 1;
        end
    end
    
    return equals;
end