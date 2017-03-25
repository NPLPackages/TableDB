--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/MessagePrinter.lua");
local MessagePrinter = commonlib.gettable("Raft.MessagePrinter");
------------------------------------------------------------
]]--
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua");
local util = commonlib.gettable("System.Compiler.lib.util")

NPL.load("(gl)script/Raft/Rpc.lua");
local Rpc = commonlib.gettable("Raft.Rpc");
local MessagePrinter = commonlib.gettable("Raft.MessagePrinter");

function MessagePrinter:new(baseDir, ip, listeningPort) 
    local o = {
        ip = ip,
        port = listeningPort,
        logger = commonlib.logging.GetLogger(""),
        -- snapshotStore = baseDir.resolve("snapshots"),
        commitIndex = 0,
        messageSender = nil,

        messages = {},
        pendingMessages = {},
    };
    setmetatable(o, self);

    if not ParaIO.CreateDirectory(baseDir) then
        o.logger.error("%s create error", baseDir)
    end
    return o;
end

function MessagePrinter:__index(name)
    return rawget(self, name) or MessagePrinter[name];
end

function MessagePrinter:__tostring()
    return util.table_tostring(self)
end


--[[
 * Starts the state machine, called by RaftConsensus, RaftConsensus will pass an instance of
 * RaftMessageSender for the state machine to send logs to cluster, so that all state machines
 * in the same cluster could be in synced
 * @param raftMessageSender
 ]]--
function MessagePrinter:start(raftMessageSender)
    self.messageSender = raftMessageSender;
    local o = self;

    -- use Rpc for incoming Request message
    Rpc:new():init("MPRequestRPC", function(self, msg) 
        -- LOG.std(nil, "info", "MPRequestRPC", msg);
        msg = o:processMessage(msg)
        return msg; 
    end)

    -- port is need to be string here??
    -- NPL.StartNetServer(self.ip, tostring(self.port));
    MPRequestRPC:MakePublic();

end
    
--[[
 * Commit the log data at the {@code logIndex}
 * @param logIndex the log index in the logStore
 * @param data 
 ]]--
function MessagePrinter:commit(logIndex, data)
    local message = data;
    print(format("commit: %d\t%s", logIndex, message));
    self.commitIndex = logIndex;
    self:addMessage(message);
end

--[[
 * Rollback a preCommit item at index {@code logIndex}
 * @param logIndex log index to be rolled back
 * @param data
 ]]--
function MessagePrinter:rollback(logIndex, data)
end

--[[
 * PreCommit a log entry at log index {@code logIndex}
 * @param logIndex the log index to commit
 * @param data
 ]]--
function MessagePrinter:preCommit(logIndex, data)
    local message = data;
    print(string.format("PreCommit:%s at %d", message, tonumber(logIndex)));

    local index = string.find(message, ':');
    if(index ~= nil) then
        key = string.sub(message, 1, index - 1);
        self.pendingMessages[key] = message;
    end
end

--[[
 * Save data for the snapshot
 * @param snapshot the snapshot information
 * @param offset offset of the data in the whole snapshot
 * @param data part of snapshot data
 ]]--
function MessagePrinter:saveSnapshotData(snapshot, offset, data)
end

--[[
 * Apply a snapshot to current state machine
 * @param snapshot
 * @return true if successfully applied, otherwise false
 ]]--
function MessagePrinter:applySnapshot(snapshot)
end

--[[
 * Read snapshot data at the specified offset to buffer and return bytes read
 * @param snapshot the snapshot info
 * @param offset the offset of the snapshot data
 * @param buffer the buffer to be filled
 * @return bytes read
 ]]--
function MessagePrinter:readSnapshotData(snapshot, offset, buffer)
end

--[[
 * Read the last snapshot information
 * @return last snapshot information in the state machine or null if none
 ]]--
function MessagePrinter:getLastSnapshot()
end

--[[
 * Create a snapshot data based on the snapshot information asynchronously
 * set the future to true if snapshot is successfully created, otherwise, 
 * set it to false
 * @param snapshot the snapshot info
 * @return true if snapshot is created successfully, otherwise false
 ]]--
function MessagePrinter:createSnapshot(snapshot)
end

--[[
 * Save the state of state machine to ensure the state machine is in a good state, then exit the system
 * this MUST exits the system to protect the safety of the algorithm
 * @param code 0 indicates the system is gracefully shutdown, -1 indicates there are some errors which cannot be recovered
 ]]--
function MessagePrinter:exit(code)
end


function MessagePrinter:processMessage(message)
    print("Got message " .. util.table_tostring(message));
    return self.messageSender:appendEntries(message);
end


function MessagePrinter:addMessage(message)
    index = string.find(message, ':');
    if(index == nil ) then
        return;
    end
    
    key = string.sub(message, 1, index-1);
    self.messages[key] = message;
    self.pendingMessages[key] = nil;

end