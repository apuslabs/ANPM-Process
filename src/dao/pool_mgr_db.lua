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

    -- This table simplifies querying current staking balances per user/pool
  -- Addresses the efficiency concern mentioned in the review.
  db_admin:exec([[
     CREATE TABLE IF NOT EXISTS user_staking_transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_wallet_address TEXT NOT NULL,
        pool_id TEXT NOT NULL,
        amount TEXT NOT NULL DEFAULT '0', -- bint string
        created_at INTEGER NOT NULL -- Timestamp of the last action affecting eligibility (stake/unstake)
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


-- ===================
-- Staking Functions
-- ===================

--- Records a staking transaction and updates the current_stakes table.
-- @param user_wallet_address User's AR address.
-- @param pool_id Target Pool ID.
-- @param transaction_type 'STAKE' or 'UNSTAKE'.
-- @param amount Amount (bint string). Positive for STAKE, negative for UNSTAKE.
function PoolMgrDAO:recordStakingTransaction(user_wallet_address, pool_id, transaction_type, amount)
  assert(type(user_wallet_address) == "string", "user_wallet_address must be a string")
  assert(type(pool_id) == "string", "pool_id must be a string")
  assert(transaction_type == 'STAKE' or transaction_type == 'UNSTAKE', "Invalid transaction_type")
  assert(type(amount) == "string", "amount must be a bint string")

  local current_time = math.floor(os.time())
  
  -- Store transaction record, only recording the operation without complex logic
  local record_sql = [[
      INSERT INTO user_staking_transactions (user_wallet_address, pool_id, transaction_type, amount, created_at)
      VALUES (?, ?, ?, ?, ?);
  ]]
  
  -- Check if amount is a valid number
  local abs_amount = amount
  if not tonumber(amount) then
      Logger.error("Invalid amount format: " .. amount)
      return false
  end
  
  local record_params = { user_wallet_address, pool_id, transaction_type, abs_amount, current_time }
  self.dbAdmin:apply(record_sql, record_params)
  
end

--- Gets the current staked amount for a user in a specific pool.
-- @param user_wallet_address User's AR address.
-- @param pool_id Pool ID.
-- @return Current staked amount (bint string), defaults to '0'.
function PoolMgrDAO:getCurrentStake(user_wallet_address, pool_id)
  assert(type(user_wallet_address) == "string", "user_wallet_address must be a string")
  assert(type(pool_id) == "string", "pool_id must be a string")

  local sql = [[ 
      SELECT transaction_type, amount 
      FROM user_staking_transactions 
      WHERE user_wallet_address = ? AND pool_id = ?; 
  ]]

  -- The order of parameters in the table must match the order of '?' in the SQL query.
  local rows = self.dbAdmin:select(sql, { user_wallet_address, pool_id })
  
  local total_staked_for_user_in_pool = '0' -- Initialize as a bint string

  if rows then
      for _, row in ipairs(rows) do
          if row.transaction_type == 'STAKE' then
              total_staked_for_user_in_pool = BintUtils.add(total_staked_for_user_in_pool, row.amount)
          elseif row.transaction_type == 'UNSTAKE' then
              total_staked_for_user_in_pool = BintUtils.sub(total_staked_for_user_in_pool, row.amount)
          end
          -- If there are other transaction_types, they are ignored by this logic.
      end
  end

  return total_staked_for_user_in_pool
end

--- Gets the total staked amount for a user across all pools.
-- @param user_wallet_address User's AR address.
-- @return Total staked amount (bint string).
function PoolMgrDAO:getTotalUserStake(user_wallet_address)
  assert(type(user_wallet_address) == "string", "user_wallet_address must be a string")

  local sql = [[ 
      SELECT transaction_type, amount 
      FROM user_staking_transactions 
      WHERE user_wallet_address = ?; 
  ]]

  -- The order of parameters in the table must match the order of '?' in the SQL query.
  local rows = self.dbAdmin:select(sql, { user_wallet_address })
  
  local total_staked_for_user = '0' -- Initialize as a bint string
  if rows then
      for _, row in ipairs(rows) do
          if row.transaction_type == 'STAKE' then
              total_staked_for_user = BintUtils.add(total_staked_for_user, row.amount)
          elseif row.transaction_type == 'UNSTAKE' then
              total_staked_for_user = BintUtils.sub(total_staked_for_user, row.amount)
          end
          -- If there are other transaction_types, they are ignored by this logic.
      end
  end

  return total_staked_for_user
end

--- Gets the total staked amount in a specific pool.
-- @param pool_id Pool ID.
-- @return Total staked amount in the pool (bint string).
function PoolMgrDAO:getTotalPoolStake(pool_id)
  assert(type(pool_id) == "string", "pool_id must be a string")
  local sql = [[ 
      SELECT transaction_type, amount 
      FROM user_staking_transactions 
      WHERE pool_id = ?; 
  ]]
  local rows = self.dbAdmin:select(sql, { pool_id })
  local total = '0'
  if rows then
      for _, row in ipairs(rows) do
          if row.transaction_type == 'STAKE' then
              total = BintUtils.add(total, row.amount)
          elseif row.transaction_type == 'UNSTAKE' then
              total = BintUtils.sub(total, row.amount)
          end
      end
  end
  return total
end

return PoolMgrDAO
