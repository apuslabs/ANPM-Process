local sqlite3 = require('lsqlite3')
local DbAdmin = require('utils.db_admin')
local Logger = require('utils.log')

-- Initialize in-memory SQLite database or reuse existing one
PoolMgrDb = PoolMgrDb or sqlite3.open_memory()

LogLevel = LogLevel or 'info'
local PoolMgrDAO = {}
PoolMgrDAO.__index = PoolMgrDAO

-- Database Initialization
local function initialize_database(db_admin)
  Logger.info('Initializing Pool Manager database schema...')
  db_admin:exec([[
    CREATE TABLE IF NOT EXISTS credits_records (
      ref INTEGER PRIMARY KEY,
      wallet_address TEXT NOT NULL,
      action TEXT NOT NULL, -- 'add', 'transfer', 'buy'
      pool_id TEXT, -- Target/Source Pool ID for transfers, '0' for buy or transfer
      quantity TEXT NOT NULL, -- bint string
      created_at INTEGER NOT NULL
    );
  ]])


  Logger.info('Pool Manager database schema initialized.')
end

--- Creates a new PoolMgrDb instance.
-- @param db_path Optional path to the database file. Defaults to in-memory.
-- @return A new PoolMgrDb instance.
function PoolMgrDAO.new()
  local self = setmetatable({}, PoolMgrDAO)
  self.dbAdmin = DbAdmin.new(PoolMgrDb)
  initialize_database(self.dbAdmin)
  return self
end

-- ===================
-- Credit Functions
-- ===================

--- Records a credit transaction and updates the current_credits table.
-- @param wallet_address User's AR address.
-- @param action Type of action ('buy', 'transfer_out', 'transfer_in').
-- @param amount Amount of credits (bint string). Positive for buy/transfer_in, negative for transfer_out.
-- @param pool_id Target/Source Pool ID for transfers, '0' for unallocated changes.
function PoolMgrDAO:recordCreditTransaction(ref,wallet_address, action, quantity, pool_id)
  assert(type(wallet_address) == "string", "wallet_address must be a string")
  assert(type(action) == "string", "action must be a string")
  assert(type(quantity) == "string", "quantity must be a bint string")
  assert(pool_id == '0' or type(pool_id) == "string", "pool_id must be '0' or a string")

  local current_time = math.floor(os.time())
  local record_sql = [[
    INSERT INTO credits_records (ref , wallet_address, action, pool_id, quantity, created_at)
    VALUES (?, ?, ?, ?, ?, ?);
  ]]
  local record_params = { ref , wallet_address, action, pool_id, quantity , current_time }
  self.dbAdmin:apply(record_sql, record_params)
end


--- Gets all credit balances for a user across all pools including unallocated.
-- @param wallet_address User's AR address.
-- @return Table mapping pool_id ('0' for unallocated) to credit balance (bint string).
function PoolMgrDAO:getUserCreditsRecords(wallet_address)
    assert(type(wallet_address) == "string", "wallet_address must be a string")
    local sql = [[ SELECT * FROM credits_records WHERE wallet_address = ? ORDER BY created_at ASC; ]]
    local results = self.dbAdmin:select(sql, { wallet_address })
    if results  and #results > 0 then
        return results
    end
    return {}
end

--- Gets all credit balances for all users (owner only).
-- @return Table mapping wallet_address to { pool_id = credits }.
function PoolMgrDAO:getAllCreditsRecords()
    local sql = [[ SELECT *  FROM credits_records ORDER BY created_at ASC; ]]
    local results = self.dbAdmin:exec(sql)
    if results  and #results > 0 then
        return results
    end
    return {}
end

return PoolMgrDAO
