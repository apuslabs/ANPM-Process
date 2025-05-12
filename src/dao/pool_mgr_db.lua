local sqlite3 = require('lsqlite3')
local DbAdmin = require('utils.db_admin')
local Logger = require('utils.log')
local BintUtils = require('utils.bint_utils')
local json = require('json')
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
        transaction_type TEXT NOT NULL, -- 'STAKE', 'UNSTAKE'
        amount TEXT NOT NULL DEFAULT '0', -- bint string
        created_at INTEGER NOT NULL -- Timestamp of the last action affecting eligibility (stake/unstake)
     );
  ]])
  db_admin:exec([[  
    -- Represents the current, individual, active stake portions for aging
    CREATE TABLE IF NOT EXISTS user_active_stake_portions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_wallet_address TEXT NOT NULL,
      pool_id TEXT NOT NULL,
      staked_amount TEXT NOT NULL, -- The current remaining amount of this specific stake portion
      staked_at INTEGER NOT NULL, -- Timestamp when this specific portion was originally staked
      -- Optional: Link back to the original STAKE transaction for auditing
      -- original_transaction_id INTEGER, 
      -- FOREIGN KEY (original_transaction_id) REFERENCES user_staking_transactions(id)
      CONSTRAINT uq_user_pool_staked_at UNIQUE (user_wallet_address, pool_id, staked_at) -- Helps prevent duplicate entries if logic error
    );
  
    -- Index to speed up fetching portions for a user/pool, ordered by time (FIFO for unstaking)
    CREATE INDEX IF NOT EXISTS idx_user_active_stakes_user_pool_time 
    ON user_active_stake_portions (user_wallet_address, pool_id, staked_at);
  ]])

  db_admin:exec([[
    CREATE TABLE IF NOT EXISTS interest_distributions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_wallet_address TEXT NOT NULL,
      pool_id TEXT NOT NULL,
      amount TEXT NOT NULL, -- bint string (interest amount)
      stake_amount TEXT NOT NULL, -- bint string (stake amount at time of distribution)
      distribution_time INTEGER NOT NULL
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
-- Staking Functionss
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

  -- assert(type(user_wallet_address) == "string", "user_wallet_address must be a string")
  -- assert(type(pool_id) == "string", "pool_id must be a string")
  -- assert(transaction_type == 'STAKE' or transaction_type == 'UNSTAKE', "Invalid transaction_type")
  -- assert(type(amount) == "string", "amount must be a bint string")


  --   local current_time = math.floor(os.time())

  --   -- Start DB Transaction
  --   self.dbAdmin:begin_transaction() -- IMPORTANT!

  --   local success = true
  --   local error_msg = nil

  --   -- 1. Log the transaction
  --   local record_sql = [[
  --       INSERT INTO user_staking_transactions (user_wallet_address, pool_id, transaction_type, amount, created_at)
  --       VALUES (?, ?, ?, ?, ?);
  --   ]]
  --   local record_params = { user_wallet_address, pool_id, transaction_type, amount, current_time }
  --   local ok, err = self.dbAdmin:apply(record_sql, record_params)
  --   if not ok then
  --       success = false
  --       error_msg = "Failed to record transaction: " .. (err or "unknown error")
  --       -- Rollback will happen below
  --   end

  --   -- 2. Update user_active_stake_portions
  --   if success then
  --       if transaction_type == 'STAKE' then
  --           -- For STAKE, add a new portion
  --           if not BintUtils.isZero(amount) then -- Only add if amount > 0
  --               local add_portion_sql = [[
  --                   INSERT INTO user_active_stake_portions (user_wallet_address, pool_id, staked_amount, staked_at)
  --                   VALUES (?, ?, ?, ?);
  --               ]]
  --               -- Note: If a stake happens at the exact same second as a previous one,
  --               -- the UNIQUE constraint (user_wallet_address, pool_id, staked_at) might fail.
  --               -- If this is a concern, the constraint needs adjustment or this logic needs to handle it
  --               -- (e.g., by checking and merging if it's an "add to existing same-second stake").
  --               -- For simplicity here, we assume distinct staked_at or the constraint is removed/modified.
  --               local add_params = { user_wallet_address, pool_id, amount, current_time }
  --               ok, err = self.dbAdmin:apply(add_portion_sql, add_params)
  --               if not ok then
  --                   success = false
  --                   error_msg = "Failed to add active stake portion: " .. (err or "unknown error")
  --               end
  --           end

  --       elseif transaction_type == 'UNSTAKE' then
  --           if not BintUtils.isZero(amount) then -- Only process if amount to unstake > 0
  --               local amount_to_unstake = amount
                
  --               -- Fetch oldest stake portions for this user/pool (FIFO)
  --               local select_portions_sql = [[
  --                   SELECT id, staked_amount 
  --                   FROM user_active_stake_portions
  --                   WHERE user_wallet_address = ? AND pool_id = ?
  --                   ORDER BY staked_at ASC;
  --               ]]
  --               local portions = self.dbAdmin:select(select_portions_sql, { user_wallet_address, pool_id })

  --               if portions then
  --                   for _, portion in ipairs(portions) do
  --                       if BintUtils.isZero(amount_to_unstake) or BintUtils.lt(amount_to_unstake, '0') then 
  --                           break -- All unstaked or error
  --                       end

  --                       local portion_id = portion.id
  --                       local portion_amount = portion.staked_amount

  --                       if BintUtils.gte(portion_amount, amount_to_unstake) then
  --                           -- This portion covers the remaining amount to unstake
  --                           local new_portion_amount = BintUtils.sub(portion_amount, amount_to_unstake)
  --                           amount_to_unstake = '0' -- All has been unstaked

  --                           if BintUtils.isZero(new_portion_amount) then
  --                               -- Fully consumed this portion, delete it
  --                               local delete_sql = "DELETE FROM user_active_stake_portions WHERE id = ?;"
  --                               ok, err = self.dbAdmin:apply(delete_sql, { portion_id })
  --                           else
  --                               -- Partially consumed, update it
  --                               local update_sql = "UPDATE user_active_stake_portions SET staked_amount = ? WHERE id = ?;"
  --                               ok, err = self.dbAdmin:apply(update_sql, { new_portion_amount, portion_id })
  --                           end
  --                       else
  --                           -- This portion is fully consumed by the unstake
  --                           amount_to_unstake = BintUtils.sub(amount_to_unstake, portion_amount)
  --                           local delete_sql = "DELETE FROM user_active_stake_portions WHERE id = ?;"
  --                           ok, err = self.dbAdmin:apply(delete_sql, { portion_id })
  --                       end

  --                       if not ok then
  --                           success = false
  --                           error_msg = "Failed to update/delete active stake portion: " .. (err or "unknown error")
  --                           break -- Stop processing on error
  --                       end
  --                   end
  --               end

  --               -- If after processing all portions, amount_to_unstake is still > 0, it means insufficient stake
  --               if success and BintUtils.gt(amount_to_unstake, '0') then
  --                   success = false
  --                   error_msg = "Insufficient staked balance to unstake " .. amount_str .. ". Remaining to unstake: " .. amount_to_unstake
  --                   Logger.warn(error_msg .. " for user " .. user_wallet_address .. " in pool " .. pool_id)
  --                   -- This is a critical data integrity issue if it happens, means over-unstaking
  --                   -- The transaction will be rolled back.
  --               end
  --           end
  --       end
  --   end

  --   -- Commit or Rollback DB Transaction
  --   if success then
  --       self.dbAdmin:commit_transaction()
  --       return true
  --   else
  --       self.dbAdmin:rollback_transaction()
  --       Logger.error("Staking transaction failed for " .. user_wallet_address .. ": " .. error_msg)
  --       return false, error_msg
  --   end
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
              total_staked_for_user_in_pool = BintUtils.subtract(total_staked_for_user_in_pool, row.amount)
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
              total_staked_for_user = BintUtils.subtract(total_staked_for_user, row.amount)
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
  assert(pool_id == '0' or type(pool_id) == "string", "pool_id must be '0' or a string")
  local sql = [[ 
      SELECT transaction_type, amount 
      FROM user_staking_transactions 
      WHERE pool_id = ?; 
  ]]
  local results = self.dbAdmin:select(sql, { pool_id })
  Logger.info('getTotalPoolStake results: ' .. json.encode(results))
  local total = '0'
  if results then
      for _, row in ipairs(results) do
          if row.transaction_type == 'STAKE' then
              total = BintUtils.add(total, row.amount)
          elseif row.transaction_type == 'UNSTAKE' then
              total = BintUtils.subtract(total, row.amount)
          end
      end
  end
  return total
end

--- Gets all staking records from the database.
-- @return Table containing all staking transaction records.
function PoolMgrDAO:getAllStakeRecords()
  local sql = [[ 
      SELECT * FROM user_staking_transactions
      ORDER BY created_at ASC;
  ]]
  local results = self.dbAdmin:exec(sql)
  if not results then
      results = {}
  end
  return results
end

return PoolMgrDAO
