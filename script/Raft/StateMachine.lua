--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/StateMachine.lua");
local StateMachine = commonlib.gettable("Raft.StateMachine");
------------------------------------------------------------
]]--


local StateMachine = commonlib.gettable("Raft.StateMachine");

function StateMachine:new() 
    local o = {
    };
    setmetatable(o, self);
    return o;
end

function StateMachine:__index(name)
    return rawget(self, name) or StateMachine[name];
end

function StateMachine:__tostring()
    return util.table_tostring(self)
end


--[[
   Starts the state machine, called by RaftConsensus, RaftConsensus will pass an instance of
   RaftMessageSender for the state machine to send logs to cluster, so that all state machines
   in the same cluster could be in synced
   @param raftMessageSender
 ]]--
function StateMachine:start(raftMessageSender)
end

--[[
   Commit the log data at the {@code logIndex}
   @param logIndex the log index in the logStore
   @param data 
 ]]--
function StateMachine:commit(logIndex, data)
end

--[[
   Rollback a preCommit item at index {@code logIndex}
   @param logIndex log index to be rolled back
   @param data
 ]]--
function StateMachine:rollback(logIndex, data)
end

--[[
   PreCommit a log entry at log index {@code logIndex}
   @param logIndex the log index to commit
   @param data
 ]]--
function StateMachine:preCommit(logIndex, data)
end

 --[[
  Save data for the snapshot
  @param snapshot the snapshot information
  @param offset offset of the data in the whole snapshot
  @param data part of snapshot data
]]--
function StateMachine:saveSnapshotData(snapshot, offset, data)
end

--[[
   Apply a snapshot to current state machine
   @param snapshot
   @return true if successfully applied, otherwise false
 ]]--
function StateMachine:applySnapshot(snapshot)
end

--[[
   Read snapshot data at the specified offset to buffer and return bytes read
   @param snapshot the snapshot info
   @param offset the offset of the snapshot data
   @param buffer the buffer to be filled
   @param expectedSize the expectedSize to be filled
   @return bytes read
 ]]--
function StateMachine:readSnapshotData(snapshot, offset, buffer, expectedSize)
end

--[[
   Read the last snapshot information
   @return last snapshot information in the state machine or null if none
 ]]--
function StateMachine:getLastSnapshot()
end

--[[
   Create a snapshot data based on the snapshot information asynchronously
   set the future to true if snapshot is successfully created, otherwise, 
   set it to false
   @param snapshot the snapshot info
   @return true if snapshot is created successfully, otherwise false
 ]]--
function StateMachine:createSnapshot(snapshot)
end
    
--[[
   Save the state of state machine to ensure the state machine is in a good state, then exit the system
   this MUST exits the system to protect the safety of the algorithm
   @param code 0 indicates the system is gracefully shutdown, -1 indicates there are some errors which cannot be recovered
 ]]--
function StateMachine:exit(code)
end