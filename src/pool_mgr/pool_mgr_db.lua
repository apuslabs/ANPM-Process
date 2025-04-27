local sqlite3 = require('lsqlite3')
local DbAdmin = require('utils.db_admin')
local BintUtils = require('utils.bint_utils')
local Logger = require('utils.log')
local config = require('pool_mgr.config') -- Use relative path from src

local PoolMgrDb = {}
PoolMgrDb.__index = PoolMgrDb

-- Database Initialization
local function initialize_database(db_admin)
  Logger.log('Initializing Pool Manager database schema...')
  db_admin:exec([[
    CREATE TABLE IF NOT EXISTS credits_records (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      wallet_address TEXT NOT NULL,
      action TEXT NOT NULL, -- 'buy', 'transfer_out', 'transfer_in' (from pool refund?)
      pool_id TEXT, -- Target/Source Pool ID for transfers, NULL for buy
      amount TEXT NOT NULL, -- bint string
      created_at INTEGER NOT NULL
    );
  ]])

  db_admin:exec([[
    CREATE TABLE IF NOT EXISTS pools (
      pool_id TEXT PRIMARY KEY, -- The AO Process ID of the Pool
      creator TEXT NOT NULL, -- AR address of the creator
      staking_capacity TEXT NOT NULL, -- bint string
      rewards_amount TEXT NOT NULL, -- bint string (Daily 'R' value for this pool)
      created_at INTEGER NOT NULL,
      started_at INTEGER -- Timestamp when staking/rewards become active? LLD says NOT NULL, but might be set later. Defaulting to NULL for now.
    );
  ]])

  db_admin:exec([[
    CREATE TABLE IF NOT EXISTS user_staking_transactions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_wallet_address TEXT NOT NULL,
      pool_id TEXT NOT NULL, -- Target Pool ID for staking
      transaction_type TEXT NOT NULL, -- 'STAKE', 'UNSTAKE'
      amount TEXT NOT NULL, -- bint string
      created_at INTEGER NOT NULL
    );
  ]])

  db_admin:exec([[
    CREATE TABLE IF NOT EXISTS interest_distributions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_wallet_address TEXT NOT NULL,
      pool_id TEXT NOT NULL,
      amount TEXT NOT NULL, -- bint string (interest amount)
      stake_amount TEXT NOT NULL, -- bint string (stake amount at time of distribution)
      daily_rate TEXT NOT NULL, -- string representation of the rate
      distribution_time INTEGER NOT NULL
    );
  ]])

  -- This table simplifies querying current credit balances per user/pool
  db_admin:exec([[
    CREATE TABLE IF NOT EXISTS current_credits (
       wallet_address TEXT NOT NULL,
       pool_id TEXT NOT NULL, -- '0' for unallocated credits
       credits TEXT NOT NULL DEFAULT '0', -- bint string
       PRIMARY KEY (wallet_address, pool_id)
    );
  ]])

  -- This table simplifies querying current staking balances per user/pool
  -- Addresses the efficiency concern mentioned in the review.
  db_admin:exec([[
     CREATE TABLE IF NOT EXISTS current_stakes (
        user_wallet_address TEXT NOT NULL,
        pool_id TEXT NOT NULL,
        amount TEXT NOT NULL DEFAULT '0', -- bint string
        last_stake_timestamp INTEGER, -- Timestamp of the last action affecting eligibility (stake/unstake)
        PRIMARY KEY (user_wallet_address, pool_id)
     );
  ]])


  Logger.log('Pool Manager database schema initialized.')
end

--- Creates a new PoolMgrDb instance.
-- @param db_path Optional path to the database file. Defaults to in-memory.
-- @return A new PoolMgrDb instance.
function PoolMgrDb.new(db_path)
  local self = setmetatable({}, PoolMgrDb)
  db_path = db_path or ':memory:' -- Use config.DbName for persistent storage?
  local db = sqlite3.open(config.DbName) -- Use db name from config
  self.dbAdmin = DbAdmin.new(db)
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
function PoolMgrDb:recordCreditTransaction(wallet_address, action, amount, pool_id)
  assert(type(wallet_address) == "string", "wallet_address must be a string")
  assert(type(action) == "string", "action must be a string")
  assert(type(amount) == "string", "amount must be a bint string")
  assert(pool_id == '0' or type(pool_id) == "string", "pool_id must be '0' or a string")

  local current_time = math.floor(os.time())
  local record_sql = [[
    INSERT INTO credits_records (wallet_address, action, amount, pool_id, created_at)
    VALUES (?, ?, ?, ?, ?);
  ]]
  local record_params = { wallet_address, action, amount, (pool_id ~= '0' and pool_id or nil), current_time }
  local success, _ = self.dbAdmin:insert(record_sql, record_params)
  if not success then
      Logger.error("Failed to record credit transaction for " .. wallet_address)
      return false
  end

  -- Update current_credits
  local update_sql = [[
    INSERT INTO current_credits (wallet_address, pool_id, credits)
    VALUES (?, ?, ?)
    ON CONFLICT(wallet_address, pool_id) DO UPDATE SET
      credits = CAST(credits AS TEXT) + CAST(? AS TEXT); -- Needs Bint addition
      -- SQLite doesn't have built-in BigInt. We need to read, add using BintUtils, then write.
  ]]

  -- Read current credits
  local read_sql = [[ SELECT credits FROM current_credits WHERE wallet_address = ? AND pool_id = ?; ]]
  local current_credit_rows = self.dbAdmin:select(read_sql, { wallet_address, pool_id })
  local current_credit_val = '0'
  if current_credit_rows and #current_credit_rows > 0 then
      current_credit_val = current_credit_rows[1].credits
  end

  -- Calculate new balance
  local new_balance = BintUtils.add(current_credit_val, amount)

  -- Upsert new balance
  local upsert_sql = [[
     INSERT INTO current_credits (wallet_address, pool_id, credits)
     VALUES (?, ?, ?)
     ON CONFLICT(wallet_address, pool_id) DO UPDATE SET credits = ?;
  ]]
  local upsert_params = { wallet_address, pool_id, new_balance, new_balance }
  local changes = self.dbAdmin:apply(upsert_sql, upsert_params)

  if changes > 0 then
      Logger.log("Updated credits for " .. wallet_address .. " in pool " .. pool_id .. " to " .. new_balance)
      return true
  else
      -- This might happen if the insert succeeded but update failed, which is unlikely with upsert.
      -- Or if BintUtils failed? Need robust error handling.
      Logger.error("Failed to update current_credits for " .. wallet_address .. " in pool " .. pool_id)
      -- Consider rolling back the transaction record if possible. DbAdmin doesn't expose transactions yet.
      return false
  end
end


--- Gets the current credit balance for a user in a specific pool ('0' for unallocated).
-- @param wallet_address User's AR address.
-- @param pool_id Pool ID ('0' for unallocated).
-- @return Current credit balance (bint string), defaults to '0'.
function PoolMgrDb:getCurrentCredits(wallet_address, pool_id)
    assert(type(wallet_address) == "string", "wallet_address must be a string")
    assert(pool_id == '0' or type(pool_id) == "string", "pool_id must be '0' or a string")
    local sql = [[ SELECT credits FROM current_credits WHERE wallet_address = ? AND pool_id = ?; ]]
    local rows = self.dbAdmin:select(sql, { wallet_address, pool_id })
    if rows and #rows > 0 then
        return rows[1].credits
    else
        return '0'
    end
end

--- Gets all credit balances for a user across all pools including unallocated.
-- @param wallet_address User's AR address.
-- @return Table mapping pool_id ('0' for unallocated) to credit balance (bint string).
function PoolMgrDb:getAllUserCredits(wallet_address)
    assert(type(wallet_address) == "string", "wallet_address must be a string")
    local sql = [[ SELECT pool_id, credits FROM current_credits WHERE wallet_address = ? AND credits != '0'; ]]
    local rows = self.dbAdmin:select(sql, { wallet_address })
    local credits = {}
    if rows then
        for _, row in ipairs(rows) do
            credits[row.pool_id] = row.credits
        end
    end
    -- Ensure unallocated is present even if zero
    if not credits['0'] then
        credits['0'] = '0'
    end
    return credits
end

--- Gets all credit balances for all users (owner only).
-- @return Table mapping wallet_address to { pool_id = credits }.
function PoolMgrDb:getAllCredits()
    local sql = [[ SELECT wallet_address, pool_id, credits FROM current_credits WHERE credits != '0'; ]]
    local rows = self.dbAdmin:select(sql, {})
    local all_credits = {}
    if rows then
        for _, row in ipairs(rows) do
            if not all_credits[row.wallet_address] then
                all_credits[row.wallet_address] = {}
            end
            all_credits[row.wallet_address][row.pool_id] = row.credits
        end
    end
    return all_credits
end


-- ===================
-- Pool Functions
-- ===================

--- Adds a new pool to the database.
-- @param pool_id The AO Process ID of the new pool.
-- @param creator The AR address of the pool creator.
-- @param staking_capacity Max staking amount (bint string).
-- @param rewards_amount Daily rewards ('R') for the pool (bint string).
-- @return True if successful, false otherwise.
function PoolMgrDb:addPool(pool_id, creator, staking_capacity, rewards_amount)
    assert(type(pool_id) == "string", "pool_id must be a string")
    assert(type(creator) == "string", "creator must be a string")
    assert(type(staking_capacity) == "string", "staking_capacity must be a bint string")
    assert(type(rewards_amount) == "string", "rewards_amount must be a bint string")
    local current_time = math.floor(os.time())
    local sql = [[
        INSERT INTO pools (pool_id, creator, staking_capacity, rewards_amount, created_at)
        VALUES (?, ?, ?, ?, ?);
    ]]
    -- started_at is initially NULL
    local params = { pool_id, creator, staking_capacity, rewards_amount, current_time }
    local success, _ = self.dbAdmin:insert(sql, params)
    if success then
        Logger.log("Pool " .. pool_id .. " created by " .. creator)
        return true
    else
        Logger.error("Failed to add pool " .. pool_id)
        return false
    end
end

--- Gets details for a specific pool.
-- @param pool_id The AO Process ID of the pool.
-- @return Pool details table or nil if not found.
function PoolMgrDb:getPool(pool_id)
    assert(type(pool_id) == "string", "pool_id must be a string")
    local sql = [[ SELECT * FROM pools WHERE pool_id = ?; ]]
    local rows = self.dbAdmin:select(sql, { pool_id })
    if rows and #rows > 0 then
        return rows[1]
    else
        return nil
    end
end

--- Gets all registered pools.
-- @return List of pool details tables.
function PoolMgrDb:getAllPools()
    local sql = [[ SELECT * FROM pools; ]]
    return self.dbAdmin:select(sql, {})
end

-- ===================
-- Staking Functions
-- ===================

--- Records a staking transaction and updates the current_stakes table.
-- @param user_wallet_address User's AR address.
-- @param pool_id Target Pool ID.
-- @param transaction_type 'STAKE' or 'UNSTAKE'.
-- @param amount Amount (bint string). Positive for STAKE, negative for UNSTAKE.
-- @return True if successful, false otherwise.
function PoolMgrDb:recordStakingTransaction(user_wallet_address, pool_id, transaction_type, amount)
    assert(type(user_wallet_address) == "string", "user_wallet_address must be a string")
    assert(type(pool_id) == "string", "pool_id must be a string")
    assert(transaction_type == 'STAKE' or transaction_type == 'UNSTAKE', "Invalid transaction_type")
    assert(type(amount) == "string", "amount must be a bint string")

    local current_time = math.floor(os.time())
    local record_sql = [[
        INSERT INTO user_staking_transactions (user_wallet_address, pool_id, transaction_type, amount, created_at)
        VALUES (?, ?, ?, ?, ?);
    ]]
    -- Store absolute amount in transaction log
    local abs_amount = amount
    if transaction_type == 'UNSTAKE' then
       abs_amount = BintUtils.mul(amount, '-1') -- Store positive amount for unstake log
    end
    local record_params = { user_wallet_address, pool_id, transaction_type, abs_amount, current_time }
    local success, _ = self.dbAdmin:insert(record_sql, record_params)
    if not success then
        Logger.error("Failed to record staking transaction for " .. user_wallet_address)
        return false
    end

    -- Update current_stakes
    local read_sql = [[ SELECT amount FROM current_stakes WHERE user_wallet_address = ? AND pool_id = ?; ]]
    local current_stake_rows = self.dbAdmin:select(read_sql, { user_wallet_address, pool_id })
    local current_stake_val = '0'
    if current_stake_rows and #current_stake_rows > 0 then
        current_stake_val = current_stake_rows[1].amount
    end

    -- Calculate new balance
    local effective_amount = amount -- Positive for STAKE
    if transaction_type == 'UNSTAKE' then
        effective_amount = BintUtils.mul(amount, '-1') -- Negative for UNSTAKE calculation
    end
    local new_balance = BintUtils.add(current_stake_val, effective_amount)

    -- Ensure balance doesn't go negative
    if BintUtils.lt(new_balance, '0') then
        Logger.error("Unstake amount exceeds staked balance for " .. user_wallet_address .. " in pool " .. pool_id)
        -- TODO: Rollback transaction record?
        return false -- Or should have been checked before calling? Assume checked before.
        -- For safety, let's just set balance to 0 if it goes negative.
        new_balance = '0'
    end

    -- Upsert new balance and update timestamp
    local upsert_sql = [[
       INSERT INTO current_stakes (user_wallet_address, pool_id, amount, last_stake_timestamp)
       VALUES (?, ?, ?, ?)
       ON CONFLICT(user_wallet_address, pool_id) DO UPDATE SET
         amount = ?,
         last_stake_timestamp = ?;
    ]]
    local upsert_params = { user_wallet_address, pool_id, new_balance, current_time, new_balance, current_time }
    local changes = self.dbAdmin:apply(upsert_sql, upsert_params)

    if changes > 0 then
        Logger.log("Updated stake for " .. user_wallet_address .. " in pool " .. pool_id .. " to " .. new_balance)
        return true
    else
        Logger.error("Failed to update current_stakes for " .. user_wallet_address .. " in pool " .. pool_id)
        -- TODO: Rollback transaction record?
        return false
    end
end

--- Gets the current staked amount for a user in a specific pool.
-- @param user_wallet_address User's AR address.
-- @param pool_id Pool ID.
-- @return Current staked amount (bint string), defaults to '0'.
function PoolMgrDb:getCurrentStake(user_wallet_address, pool_id)
    assert(type(user_wallet_address) == "string", "user_wallet_address must be a string")
    assert(type(pool_id) == "string", "pool_id must be a string")
    local sql = [[ SELECT amount FROM current_stakes WHERE user_wallet_address = ? AND pool_id = ?; ]]
    local rows = self.dbAdmin:select(sql, { user_wallet_address, pool_id })
    if rows and #rows > 0 then
        return rows[1].amount
    else
        return '0'
    end
end

--- Gets the total staked amount for a user across all pools.
-- @param user_wallet_address User's AR address.
-- @return Total staked amount (bint string).
function PoolMgrDb:getTotalUserStake(user_wallet_address)
    assert(type(user_wallet_address) == "string", "user_wallet_address must be a string")
    local sql = [[ SELECT amount FROM current_stakes WHERE user_wallet_address = ?; ]]
    local rows = self.dbAdmin:select(sql, { user_wallet_address })
    local total = '0'
    if rows then
        for _, row in ipairs(rows) do
            total = BintUtils.add(total, row.amount)
        end
    end
    return total
end

--- Gets the total staked amount in a specific pool.
-- @param pool_id Pool ID.
-- @return Total staked amount in the pool (bint string).
function PoolMgrDb:getTotalPoolStake(pool_id)
    assert(type(pool_id) == "string", "pool_id must be a string")
    local sql = [[ SELECT amount FROM current_stakes WHERE pool_id = ?; ]]
    local rows = self.dbAdmin:select(sql, { pool_id })
    local total = '0'
    if rows then
        for _, row in ipairs(rows) do
            total = BintUtils.add(total, row.amount)
        end
    end
    return total
end

--- Gets all current stakes (owner only).
-- @return List of tables { user_wallet_address, pool_id, amount, last_stake_timestamp }.
function PoolMgrDb:getAllCurrentStakes()
    local sql = [[ SELECT * FROM current_stakes WHERE amount != '0'; ]]
    return self.dbAdmin:select(sql, {})
end

--- Gets all staking transaction records (owner only).
-- @return List of all staking transaction records.
function PoolMgrDb:getAllStakingTransactions()
    local sql = [[ SELECT * FROM user_staking_transactions ORDER BY created_at DESC; ]]
    return self.dbAdmin:select(sql, {})
end


-- ===================
-- Incentive Functions
-- ===================

--- Records an interest distribution event.
-- @param user_wallet_address User's AR address.
-- @param pool_id Pool ID where stake was held.
-- @param amount Interest amount distributed (bint string).
-- @param stake_amount User's stake amount at time of distribution (bint string).
-- @param daily_rate The calculated daily rate (string).
-- @return True if successful, false otherwise.
function PoolMgrDb:recordInterestDistribution(user_wallet_address, pool_id, amount, stake_amount, daily_rate)
    assert(type(user_wallet_address) == "string", "user_wallet_address must be a string")
    assert(type(pool_id) == "string", "pool_id must be a string")
    assert(type(amount) == "string", "amount must be a bint string")
    assert(type(stake_amount) == "string", "stake_amount must be a bint string")
    assert(type(daily_rate) == "string", "daily_rate must be a string")

    local current_time = math.floor(os.time())
    local sql = [[
        INSERT INTO interest_distributions (user_wallet_address, pool_id, amount, stake_amount, daily_rate, distribution_time)
        VALUES (?, ?, ?, ?, ?, ?);
    ]]
    local params = { user_wallet_address, pool_id, amount, stake_amount, daily_rate, current_time }
    local success, _ = self.dbAdmin:insert(sql, params)
    if success then
        Logger.log("Recorded interest distribution for " .. user_wallet_address .. " in pool " .. pool_id .. ": " .. amount)
        return true
    else
        Logger.error("Failed to record interest distribution for " .. user_wallet_address .. " in pool " .. pool_id)
        return false
    end
end

--- Gets stakes eligible for rewards (staked for > 24 hours).
-- @param pool_id The Pool ID to check eligibility for.
-- @param current_timestamp The current timestamp for comparison.
-- @return List of tables { user_wallet_address, amount, last_stake_timestamp } for eligible stakes.
function PoolMgrDb:getEligibleStakesForPool(pool_id, current_timestamp)
    assert(type(pool_id) == "string", "pool_id must be a string")
    assert(type(current_timestamp) == "number", "current_timestamp must be a number")

    local twenty_four_hours_ago = current_timestamp - (24 * 60 * 60)

    -- Select stakes where the last action was more than 24 hours ago and amount > 0
    local sql = [[
        SELECT user_wallet_address, amount, last_stake_timestamp
        FROM current_stakes
        WHERE pool_id = ?
          AND amount != '0'
          AND last_stake_timestamp IS NOT NULL
          AND last_stake_timestamp <= ?;
    ]]
    local params = { pool_id, twenty_four_hours_ago }
    local eligible_stakes = self.dbAdmin:select(sql, params)
    return eligible_stakes or {}
end


return PoolMgrDb
