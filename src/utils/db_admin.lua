local dbAdmin = {}
dbAdmin.__index = dbAdmin

-- Function to create a new database explorer instance
function dbAdmin.new(db)
    local self = setmetatable({}, dbAdmin)
    self.db = db
    return self
end

-- Function to list all tables in the database
function dbAdmin:tables()
    local tables = {}
    for row in self.db:nrows("SELECT name FROM sqlite_master WHERE type='table';") do
        table.insert(tables, row.name)
    end
    return tables
end

-- Function to get the record count of a table
function dbAdmin:count(tableName)
    local count_query = string.format("SELECT COUNT(*) AS count FROM %s;", tableName)
    for row in self.db:nrows(count_query) do
        return row.count
    end
end

-- Function to execute a given SQL query
function dbAdmin:exec(sql)
    local results = {}
    for row in self.db:nrows(sql) do
        table.insert(results, row)
    end
    return results
end

-- Function to apply SQL INSERT, UPDATE, and DELETE statements with parameter binding
-- Returns ok (boolean) and error message (string) if failed
function dbAdmin:apply(sql, values)
    local DONE = require('lsqlite3').DONE
    assert(type(sql) == 'string', 'SQL MUST be a String')
    assert(type(values) == 'table', 'values MUST be an array of values')
    local stmt = self.db:prepare(sql)
    if not stmt then
        return false, "Failed to prepare statement: " .. self.db:errmsg()
    end
    
    stmt:bind_values(table.unpack(values))
    
    local result = stmt:step()
    stmt:finalize()
    
    if result ~= DONE then
        return false, "Statement failed: " .. self.db:errmsg()
    end
    
    return true, nil
end

-- Function to apply SQL SELECT statements with parameter binding
function dbAdmin:select(sql, values)
   local sqlite3 = require('lsqlite3')
   local DONE = sqlite3.DONE
   assert(type(sql) == 'string', 'SQL MUST be a String')
   assert(type(values) == 'table', 'values MUST be an array of values')

   local stmt = self.db:prepare(sql)
   stmt:bind_values(table.unpack(values))

   local results = {}
   while true do
       local row = stmt:step()
       if row == sqlite3.ROW then
           table.insert(results, stmt:get_named_values()) 
       elseif row == DONE then
           break
       else
           error(sql .. ' statement failed because ' .. self.db:errmsg())
       end
   end

   stmt:finalize()
   return results
end

-------------------------------------------------------------------
-- Transaction Management Methods
-------------------------------------------------------------------

--[[
  Begins a new database transaction.
  @return boolean success: True if the transaction started successfully, false otherwise.
  @return string|nil errMsg: An error message if success is false.
--]]
function dbAdmin:begin_transaction()
    -- 'BEGIN TRANSACTION;' or 'BEGIN;' are both valid.
    -- lsqlite3's db:exec() returns sqlite3.OK (0) on success for non-SELECT statements.
    local sqlite3 = require('lsqlite3')
    local rc = self.db:exec('BEGIN TRANSACTION;')
    if rc ~= sqlite3.OK then
        local errMsg = "Failed to begin transaction: " .. self.db:errmsg()
        return false, errMsg
    end
    print("Transaction begin")
    return true, nil
end

--[[
  Commits the current database transaction.
  @return boolean success: True if the transaction committed successfully, false otherwise.
  @return string|nil errMsg: An error message if success is false.
--]]
function dbAdmin:commit_transaction()
    local sqlite3 = require('lsqlite3')
    local rc = self.db:exec('COMMIT;')
    if rc ~= sqlite3.OK then
        local errMsg = "Failed to commit transaction: " .. self.db:errmsg()
        print(errMsg)
        return false, errMsg
    end
    print("Transaction committed.")
    return true, nil
end

--[[
  Rolls back the current database transaction.
  @return boolean success: True if the transaction rolled back successfully, false otherwise.
  @return string|nil errMsg: An error message if success is false.
--]]
function dbAdmin:rollback_transaction()
    local sqlite3 = require('lsqlite3')
    local rc = self.db:exec('ROLLBACK;')
    if rc ~= sqlite3.OK then
        local errMsg = "Failed to rollback transaction: " .. self.db:errmsg()
        print(errMsg)
        return false, errMsg
    end
    print("Transaction rolled back.")
    return true, nil
end


return dbAdmin