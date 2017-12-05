--[[
Title: base class for store
Author(s): liuluheng,
Date: 2017/7/31
Desc:
use the lib:
------------------------------------------------------------
NPL.load("(gl)npl_mod/TableDB/SqliteWALStore.lua");
local SqliteWALStore = commonlib.gettable("TableDB.SqliteWALStore");
------------------------------------------------------------
]]
NPL.load("(gl)script/ide/commonlib.lua")
NPL.load("(gl)script/sqlite/sqlite3.lua")
NPL.load("(gl)script/ide/System/Compiler/lib/util.lua")
local util = commonlib.gettable("System.Compiler.lib.util")
NPL.load("(gl)script/ide/System/Database/SqliteStore.lua")
local SqliteStore = commonlib.gettable("System.Database.SqliteStore")
local SqliteWALStore = commonlib.inherit(SqliteStore, commonlib.gettable("TableDB.SqliteWALStore"))
local LoggerFactory = NPL.load("(gl)npl_mod/Raft/LoggerFactory.lua")


NPL.load("(gl)script/sqlite/libluasqlite3-loader.lua")
local api, ERR, TYPE, AUTH = load_libluasqlite3()

NPL.load("(gl)script/sqlite/sqlite3.lua")


local cbWALHandlerFile = "(%s)RPC/WALHandler.lua"
local cb_thread = "raft"

SqliteWALStore.logger = LoggerFactory.getLogger("SqliteWALStore")

function SqliteWALStore:ctor()
end

function SqliteWALStore:init(collection, init_args)
  SqliteWALStore._super.init(self, collection)
  local dbName = self.kFileName
  self._db:set_wal_page_hook(
    function(page_data, pgno, nTruncate, isCommit)
      local msg = {
        rootFolder = collection:GetParent():GetRootFolder(),
        collectionName = collection:GetName(),
        page_data = page_data,
        pgno = pgno,
        nTruncate = nTruncate,
        isCommit = isCommit
      }

      NPL.activate(self:GetReplyAddress(cb_thread or "main"), msg)
      self.logger.trace("pgSize %d, pgno %d, nTruncate %d, isCommit %d", #page_data, pgno, nTruncate, isCommit)
      return 1
    end
  )

  return self
end

function SqliteWALStore:injectWALPage(query, callbackFunc)
  self.logger.info(
    "logIndex:%d, pgSize %d, pgno %d, nTruncate %d, isCommit %d",
    query.logIndex,
    #query.page_data,
    query.pgno,
    query.nTruncate,
    query.isCommit
  )
  local r = self._db:wal_inject_page(query.page_data, query.pgno, query.nTruncate, query.isCommit)
  if r ~= 0 then
    self.logger.error("%d inject failed, %d", query.logIndex, r)
  else
    self.logger.trace("injected %d", query.logIndex)
  end
  if (not self.checkpoint_timer:IsEnabled()) then
    self.checkpoint_timer:Change(self.AutoCheckPointInterval, self.AutoCheckPointInterval)
  end
end

function SqliteWALStore:backup(query, callbackFunc)
  self.logger.info("backing up %s to %s", query.srcFile, query.dstFile)
  local backup_success = true
  local backupDB = sqlite3.open(query.dstFile)
  local srcDB = sqlite3.open(query.srcFile)

  -- backup can get error
  local bu = sqlite3.backup_init(backupDB, "main", srcDB, "main")
  if bu then
    local stepResult = bu:step(-1)
    if stepResult ~= ERR.DONE then
      logger.error("back up failed")
      backup_success = false

      -- an error occured
      if stepResult == ERR.BUSY or stepResult == ERR.LOCKED then
        -- we don't retry, Raft will handle for us
        -- move the previous snapshot to current logIndex?
        -- local prevSnapshortName = getLatestSnapshotName(snapshotStore, name);
        -- if prevSnapshortName == format("%s0-0_%s_s.db", snapshotStore, name) then
        --     -- leave it
        --     logger.error("backing up the 1st snapshot %s err", filePath)
        -- else
        --     logger.info("copy %s to %s", prevSnapshortName, filePath)
        --     if not (ParaIO.CopyFile(prevSnapshortName, filePath, true)) then
        --         logger.error("copy %s to %s failed", prevSnapshortName, filePath);
        --         exit(-1);
        --     end
        -- end
      else
        self.logger.error("must be a BUG!!!")
      --   exit(-1)
      end
    end
    bu:finish()
  else
    -- A call to sqlite3_backup_init() will fail, returning NULL,
    -- if there is already a read or read-write transaction open on the destination database
    self.logger.error("backup_init failed")
    backup_success = false
  end

  bu = nil

  srcDB:close()
  backupDB:close()
  if not backup_success then
    ParaIO.DeleteFile(filePath)
    return backup_success
  end

  return backup_success
end

function SqliteWALStore:Close()
  SqliteWALStore._super.Close(self)
end

function SqliteWALStore:GetReplyAddress(cb_thread)
  return format(cbWALHandlerFile, cb_thread)
end
