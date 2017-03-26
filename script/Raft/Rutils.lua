--[[
Title: 
Author: liuluheng
Date: 2017.03.25
Desc: 


------------------------------------------------------------
NPL.load("(gl)script/Raft/Rutils.lua");
local Rutils = commonlib.gettable("Raft.Rutils");
------------------------------------------------------------
]]--


local Rutils = commonlib.gettable("Raft.Rutils");

function Rutils.table_size() 
    local o = {
    };
    setmetatable(o, self);
    return o;
end

--- total number of elements in this table.
-- Note that this is distinct from `#t`, which is the number
-- of values in the array part; this value will always
-- be greater or equal. The difference gives the size of
-- the hash part, for practical purposes. Works for any
-- object with a __pairs metamethod.
-- @tab t a table
-- @return the size
function Rutils.table_size(t)
    -- assert_arg_iterable(1,t)
    local i = 0
    for k in pairs(t) do i = i + 1 end
    return i
end
