--[[
Title: base class for store
Author(s): liuluheng,
Date: 2017/7/31
Desc:
use the lib:
------------------------------------------------------------
NPL.load("(gl)npl_mod/TableDB/test/TestSqliteWALStore.lua");
local TestSqliteWALStore = commonlib.gettable("TableDB.TestSqliteWALStore");

local config_path = data.rootFolder .. "/tabledb.config.xml";
if not ParaIO.DoesFileExist(config_path) then
    self:createTestSqliteWALStoreConfig(data.rootFolder);
    self.db:connect(data.rootFolder, function (...)
        self.logger.info("connected to %s", data.rootFolder);
    end);
end


function RaftTableDBStateMachine:createTestSqliteWALStoreConfig(rootFolder)
	NPL.load("(gl)script/ide/commonlib.lua");
	NPL.load("(gl)script/ide/LuaXML.lua");
	local config = { 
		name = "tabledb", 
		{
			name = "providers", 
			{ name = "provider", attr = { name = "sqliteWAL", type = "TableDB.TestSqliteWALStore", file = "(g1)npl_mod/TableDB/TestSqliteWALStore.lua" }, "" }
		},
		{
			name = "tables",
			{ name = "table", attr = { provider = "sqliteWAL", name = "default" } }, 
		}
	}

	local config_path = rootFolder .. "/tabledb.config.xml";
	local str = commonlib.Lua2XmlString(config, true);
	ParaIO.CreateDirectory(config_path);
	local file = ParaIO.open(config_path, "w");
	if (file:IsValid()) then
		file:WriteString(str);
		file:close();
	end
end
------------------------------------------------------------
]]
NPL.load("(gl)script/ide/commonlib.lua");
NPL.load("(gl)script/sqlite/sqlite3.lua");

NPL.load("(gl)script/ide/System/Database/SqliteStore.lua");
local SqliteStore = commonlib.gettable("System.Database.SqliteStore");
local TestSqliteWALStore = commonlib.inherit(SqliteStore, commonlib.gettable("TableDB.test.TestSqliteWALStore"));


function TestSqliteWALStore:ctor()
end


NPL.load("(gl)script/ide/System/Database/TableDatabase.lua");
local TableDatabase = commonlib.gettable("System.Database.TableDatabase");

-- this will start both db client and db server if not.
local rootFolderClone = "temp/test_sqlite_wal_store_clone/"
local collectionClone = TableDatabase:new():connect(rootFolderClone)["insertNoIndexClone"];

local logIndex = 1;
-- TODO:checkout why init is called so many times
function TestSqliteWALStore:init(collection, init_args)
    TestSqliteWALStore._super.init(self, collection);
    local rootFolder = collection:GetParent():GetRootFolder();
    collectionName = collection:GetName();

    local this = self;
    self._db:set_wal_page_hook(function(page_data, pgno, nTruncate, isCommit)
        local msg = {
            rootFolder = rootFolder,
            collectionName = collectionName,
            page_data = page_data,
            pgno = pgno,
            nTruncate = nTruncate,
            isCommit = isCommit,
        }
        print(format("wal_page_hook: pgSize %d, pgno %d, nTruncate %d, isCommit %d", #page_data, pgno, nTruncate, isCommit))

        msg.logIndex = logIndex;
        logIndex = logIndex + 1;
        this:InjectWALPage(msg);
        return 1
    end)

    return self;
end

function TestSqliteWALStore:InjectWALPage(query, callbackFunc)
    collectionClone:injectWALPage(query)
    print(format("injected %s:%s", query.rootFolder, query.collectionName))
    if (not self.checkpoint_timer:IsEnabled()) then
        self.checkpoint_timer:Change(self.AutoCheckPointInterval, self.AutoCheckPointInterval);
    end
end


function TestSqliteWALStore:Close()
    TestSqliteWALStore._super.Close(self)
    collectionClone:close()
end

