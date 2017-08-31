--[[
Title: base class for store
Author(s): liuluheng, 
Date: 2017/7/31
Desc: 
use the lib:
------------------------------------------------------------
NPL.load("(gl)npl_mod/TableDB/RaftSqliteStore.lua");
local RaftSqliteStore = commonlib.gettable("TableDB.RaftSqliteStore");
------------------------------------------------------------
]]


NPL.load("(gl)script/ide/commonlib.lua");
NPL.load("(gl)script/sqlite/sqlite3.lua");

NPL.load("(gl)script/ide/System/Database/SqliteStore.lua");
local SqliteStore = commonlib.gettable("System.Database.SqliteStore");
local RaftSqliteStore = commonlib.inherit(SqliteStore, commonlib.gettable("TableDB.RaftSqliteStore"));

local follower_dbs = {}

function RaftSqliteStore:ctor()
end

function RaftSqliteStore:init(collection, init_args)
  RaftSqliteStore._super.init(self, collection);
  local dbName = self.kFileName
  self.collection = collection;

  self._db:set_wal_page_hook(function (page_data, pgno, nTruncate, isCommit)
    local db2 = follower_dbs[collection:GetName()];
    if not db2 then
      db2 = sqlite3.open(dbName .. "2")
      db2:exec("PRAGMA journal_mode=WAL;");
      db2:exec("PRAGMA synchronous=NORMAL;");
      follower_dbs[collection:GetName()] = db2;
    end
    -- followers simply append wal, no need to worry about the meta data
    print(format("wal_page_hook: pgSize %d, pgno %d, nTruncate %d, isCommit %d", #page_data, pgno, nTruncate, isCommit))
    db2:wal_inject_page(page_data, pgno, nTruncate, isCommit)
    return 1
  end)

  self._db:set_wal_checkpoint_hook(function ()
    print("wal_checkpoint_hook")
    follower_dbs[self.collection:GetName()]:wal_checkpoint("main")
    return 1
  end)

  -- create meta data in sqliteStore
	self:Begin();
		-- drop all tables. 
		self:DropAllMetaTables();
	
		-- create all tables
		self:CreateTables();

		-- insert version infos
		local insert_stmt = assert(self._db:prepare("INSERT INTO SystemInfo (Name, Value) VALUES(?, ?)"));
		insert_stmt:bind("version", SqliteStore.kCurrentVersion);
		insert_stmt:exec();
		insert_stmt:bind("author", "NPLRuntime");
		insert_stmt:exec();
		insert_stmt:bind("name", self:GetCollection():GetName());
		insert_stmt:exec();
		insert_stmt:close();

	self:End();
	self:FlushAll();
	LOG.std(nil, "TableDB", "RaftSqliteStore", "%s is recreated for raft", self.kFileName);
	self:ValidateDB();

  return self;
end

function RaftSqliteStore:Close()
    RaftSqliteStore._super.Close(self)
    -- send close
    follower_dbs[self.collection:GetName()]:close()
    follower_dbs[self.collection:GetName()] = nil;
end
