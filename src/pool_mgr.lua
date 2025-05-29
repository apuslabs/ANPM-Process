local BintUtils        = require('utils.bint_utils')
local json             = require('json')
local Logger           = require('utils.log')
local PoolMgrDb        = require('dao.pool_mgr_db').new()
local Config           = require('utils.config')

Undistributed_Credits  = Undistributed_Credits or {}
Undistributed_Interest = Undistributed_Interest or {}
Distributed_Interest   = Distributed_Interest or {}
Pools                  = Pools or {}
Stakers                = Stakers or {}
CreditExchangeRate     = CreditExchangeRate or Config.CreditExchangeRate
InterestFromTreasure   = InterestFromTreasure or "0"

-- Constants from Config
ApusTokenId            = ApusTokenId or Config.ApusTokenId

 
-- Functions to fetch state
function getCredits()
  return json.encode(Undistributed_Credits)
end
function getDistributedInterest()
  return json.encode(Distributed_Interest)
end
function getPools()
  for pool_id, pool in pairs(Pools) do
    pool.min_apr = BintUtils.divide(Pools[pool_id].rewards_amount, Pools[pool_id].staking_capacity) * 365
    if Pools[pool_id].cur_staking == "0" then
      pool.apr = ""
    else
      pool.apr = BintUtils.divide(Pools[pool_id].rewards_amount, Pools[pool_id].cur_staking) * 365
    end
  end
  return json.encode(Pools)
end

function getStakers()
  return json.encode(Stakers)
end

function getInfo()
  return json.encode({
    credit_exchange_rate = CreditExchangeRate,
  })
end



local function isValidPool(poolId)
  return Pools[poolId] ~= nil
end
local function createPool(pool_id, creator, staking_capacity, rewards_amount, staking_start,staking_end)
  return {
    pool_id = pool_id,
    creator = creator,
    staking_capacity = staking_capacity,
    rewards_amount = rewards_amount,
    staking_start = staking_start,
    staking_end = staking_end,
    min_apr = 0,
    cur_staking = '0',
    apr = 0.05,
    name = "Qwen",
    description = "In HyperBeam’s initial implementation of the Deterministic GPU Device—enabling verifiable AI on AO. Users can earn interest by staking APUS, and they can also tap GPU computing power to perform fully on‑chain, verifiable AI inference. ",
    image_url = "https://qianwen-res.oss-accelerate-overseas.aliyuncs.com/qwen3-banner.png",
  }
end
function UpdatePoolRewards(pool_id, rewards_amount)
  local pool = Pools[pool_id]
  if pool then
    if rewards_amount then
      pool.rewards_amount = rewards_amount
      Logger.trace("Updated Pool: " .. pool_id .. ", Rewards Amount: " .. rewards_amount)
    end
  else
    Logger.warn("Pool not found: " .. pool_id)
  end
end

function UpdatePoolID(old_id, new_id)
  -- Check if the old pool ID exists
  if not Pools[old_id] then
    Logger.warn("UpdatePoolID failed:  pool ID '" .. old_id .. "' does not exist.")
    return
  end

  -- Check if the new pool ID already exists
  if Pools[new_id] then
    Logger.warn("UpdatePoolID failed: New pool ID '" .. new_id .. "' already exists.")
    return
  end

  -- Copy the pool data to the new ID and remove the old ID
  Pools[new_id] = Pools[old_id]
  Pools[new_id].pool_id = new_id
  Pools[old_id] = nil
  Send({
    device = 'patch@1.0',
    pools = getPools()
  })
  Logger.info("Successfully updated pool ID from '" .. old_id .. "' to '" .. new_id .. "'.")
end

-- Credits handlers
Handlers.add(
  "Buy-Credit",
  { Action = "Credit-Notice", ['X-An-Reason'] = "Buy-Credit", From = ApusTokenId },
  function(msg)
    local user = msg.Tags.Sender -- The user who sent the APUS
    local apus_amount = msg.Tags.Quantity
    local ref = msg.Tags["X-Reference"] or msg.Tags.Reference
    Logger.info("Processing Buy-Credit from User: " .. user .. ", APUS Quantity: " .. apus_amount)
    -- Calculate credits to add

    local credits_to_add = BintUtils.multiply(apus_amount, CreditExchangeRate)
    Logger.info("User: " ..
      user .. ", Credits to Add: " .. credits_to_add .. ", Credit Exchange Rate: " .. CreditExchangeRate)
    Undistributed_Credits[user] = BintUtils.add(Undistributed_Credits[user] or '0', credits_to_add)
    
    local pool_id
    for k, v in pairs(Pools) do
      pool_id = k
      break -- Exit loop after first item
    end
    -- Send notification to the target Pool process
    ao.send({
      Target = pool_id,
      Action = "AN-Credit-Notice", -- As defined in Pool LLD
      From = ao.id,                -- Send from this Pool Mgr process
      User = user,
      Quantity = credits_to_add
    })

    
    -- Record transaction and update balance (adds to unallocated pool '0')
    PoolMgrDb:recordCreditTransaction(ref, user, "buy", apus_amount, "0")
    msg.reply({
      Tags = { Code = "200" },
      Data = json.encode({ user = user, balance = BintUtils.toBalanceValue(Undistributed_Credits[user] or '0') })
    })
    Send({
      device = 'patch@1.0',
      credits = getCredits()
    })
  end
)

--- Handler: Transfer-Credits
-- Description: User transfer their credits from a specific pool.
Handlers.add(
  "Transfer-Credits",
  { Action = "AN-Credit-Notice" },
  function(msg)
    local user = msg.Tags.User
    local quantity = msg.Tags.Quantity
    local pool_id = msg.From
    local ref = msg.Tags["X-Reference"] or msg.Tags.Reference
    -- Validate pool_id and quantity
    assert(type(user) == 'string', 'user is required!')
    assert(type(quantity) == 'string', 'Quantity is required!')
    assert(BintUtils.gt(quantity, '0'), 'Quantity must be greater than 0')
    -- Check if the pool exists
    if not isValidPool(pool_id) then
      Logger.error("Allocate-Credits failed: Invalid PoolId " .. pool_id)
      msg.reply({ Tags = { Code = "400" }, Data = "Invalid PoolId" })
      return
    end
    local pool_balance = Undistributed_Credits[user] or '0'
    Logger.info("Processing credit transfer for " .. user .. " from pool " .. pool_id .. ", Amount: " .. quantity)


    Undistributed_Credits[user] = BintUtils.add(pool_balance, quantity)
    PoolMgrDb:recordCreditTransaction(ref, user, "transfer", quantity, pool_id)
    -- Send confirmation back to Pool
    msg.reply({ Tags = { Code = "200" } })
    Send({
      device = 'patch@1.0',
      credits = getCredits()
    })
  end
)

--- Handler: Add-Credit
-- Description: User transfer their credits to a specific pool.
Handlers.add(
  "Add-Credit",
  Handlers.utils.hasMatchingTag("Action", "Add-Credit"),
  function(msg)
    local user = msg.From
    local quantity = msg.Tags.Quantity
    local pool_id = msg.Tags.poolid
    local ref = msg.Tags["X-Reference"] or msg.Tags.Reference
    -- Validate pool_id and quantity
    assert(type(user) == 'string', 'user is required!')
    assert(type(quantity) == 'string', 'Quantity is required!')
    assert(BintUtils.gt(quantity, '0'), 'Quantity must be greater than 0')
    -- Check if the balance is sufficient
    local pool_balance = Undistributed_Credits[user]
    if BintUtils.lt(pool_balance, quantity) then
      Logger.warn("Add-Credit failed: Insufficient pool balance for pool " ..
        pool_id .. ". Balance: " .. pool_balance .. ", Requested: " .. quantity)
      msg.reply({ Tags = { Code = "403", Error = "Insufficient pool balance." } })
      return
    end
    -- Check if the pool exists
    if not isValidPool(pool_id) then
      Logger.error("Allocate-Credits failed: Invalid PoolId " .. pool_id)
      msg.reply({ Tags = { Code = "400" }, Data = "Invalid PoolId" })
      return
    end
    Logger.info("Processing credit transfer for " .. user .. " to pool " .. pool_id .. ", Amount: " .. quantity)

    Undistributed_Credits[user] = BintUtils.subtract(pool_balance, quantity)

    PoolMgrDb:recordCreditTransaction(ref, user, "add", quantity, pool_id)
    -- Send notification to the target Pool process
    ao.send({
      Target = pool_id,
      Action = "AN-Credit-Notice", -- As defined in Pool LLD
      From = ao.id,                -- Send from this Pool Mgr process
      User = user,
      Quantity = quantity
    })

    Send({
      device = 'patch@1.0',
      credits = getCredits()
    })
  end
)

--- Handler: receive Interests 
-- Description: record received interest from treasure wallet_address
Handlers.add(
  "Receive-Interests",
  function(msg) return (msg.Tags.Action == 'Credit-Notice') and (msg.Tags.Sender == Config.TreasureWallet) end,
  function(msg)
    local user = msg.Tags.Sender -- The user who sent the APUS
    local apus_amount = msg.Tags.Quantity
    Logger.info("recivced apus from TreasureWallet: " .. user .. ", APUS Quantity: " .. apus_amount)
    -- assert Token is APUS
    assert(msg.From == ApusTokenId, 'Invalid Token')
    --  add InterestFromTreasure
    InterestFromTreasure = BintUtils.add(InterestFromTreasure or '0', apus_amount)
    msg.reply({
      Tags = { Code = "200" },
      Data = json.encode({ interestfromtreasure = InterestFromTreasure})
    })
  end
)
-- ================= Staking Handlers =================
-- Sends APUS tokens from this process to a recipient
local function sendApus(recipient, amount, reason)
  if BintUtils.le(amount, '0') then
    Logger.warn("sendApus: Attempted to send non-positive amount: " .. amount)
    return
  end
  Logger.info("Sending " .. amount .. " APUS to " .. recipient .. " for reason: " .. reason)
  ao.send({
    Target = ApusTokenId,
    Action = "Transfer",
    Recipient = recipient,
    Quantity = amount,
    ['X-AN-Reason'] = reason -- Add reason for context
  })
end

Handlers.add(
  "Mgr-Stake",
  { Action = "Credit-Notice", ['X-An-Reason'] = "Stake", From = ApusTokenId },
  function(msg)
    local pool_id = msg.Tags["X-poolid"]
    local user = msg.Tags.Sender -- The user who sent the APUS
    local apus_amount = msg.Tags.Quantity


    assert(type(pool_id) == 'string', 'Missing X-PoolId')
    -- Check if pool exists and get capacity
    local pool = Pools[pool_id]
    if not pool then
      Logger.error("Stake failed: Pool " .. pool_id .. " not found.")
      ao.send({ Target = user, Tags = { Code = "404", Error = "Target pool for staking not found.", Action = "Stake-Failure" } })
      return
    end
    Logger.info("Processing Stake from User: " .. user .. ", APUS Quantity: " .. apus_amount .. ", Pool: " .. pool_id)

    -- Check staking capacity
    local current_pool_stake = Pools[pool_id].cur_staking
    Logger.info("Current Pool Stake: " .. current_pool_stake)
    local capacity = pool.staking_capacity
    local potential_new_total = BintUtils.add(current_pool_stake, apus_amount)


    if BintUtils.gt(potential_new_total, capacity) then
      Logger.error("Stake failed: Staking amount " ..
        apus_amount ..
        " exceeds pool " .. pool_id .. " capacity (" .. capacity .. "). Current stake: " .. current_pool_stake)
      sendApus(user, apus_amount, "Refund: Stake Exceeds Pool Capacity")
      ao.send({ Target = user, Tags = { Code = "403", Error = "Staking amount exceeds pool capacity.", Action = "Stake-Failure" } })
      return
    end

    -- Add pool cur_staking
    Pools[pool_id].cur_staking = potential_new_total
    Logger.info("here " .. current_pool_stake)
    -- Add user staking amount
    if Stakers[user] == nil then
      Stakers[user] = {}
    end
    Stakers[user][pool_id] = BintUtils.add(Stakers[user][pool_id] or '0', apus_amount)
    -- Record staking transaction
    PoolMgrDb:recordStakingTransaction(user, pool_id, 'STAKE', apus_amount)

    local new_stake_balance = Stakers[user][pool_id]
    Logger.info("Stake successful for " .. user .. " in pool " .. pool_id .. ". New stake balance: " .. new_stake_balance)
    ao.send({
      Target = user,
      Tags = { Code = "200", Action = "Stake-Success" },
      Data = json.encode({
        pool_id = pool_id,
        staked_amount =
            apus_amount,
        new_stake_balance = new_stake_balance
      })
    })
    Send({
      device = 'patch@1.0',
      pools = getPools(),
      stakers = getStakers()
    })
  end
)

Handlers.add(
  "Mgr-UnStake",
  Handlers.utils.hasMatchingTag("Action", "UnStake"),
  function(msg)
    local user = msg.From
    local pool_id = msg.Tags.poolid
    local amount_to_unstake = msg.Tags.Quantity
    local pool = Pools[pool_id]
    -- Validate input
    if not pool_id or type(pool_id) ~= "string" or not pool then
      Logger.error("UnStake failed: Invalid pool_id from " .. user)
      msg.reply({ Tags = { Code = "400", Error = "Invalid pool_id provided.", Action = "UnStake-Failure" } })
      return
    end
    if not amount_to_unstake or BintUtils.le(amount_to_unstake, '0') then
      Logger.error("UnStake failed: Invalid amount from " .. user)
      msg.reply({ Tags = { Code = "400", Error = "Invalid amount provided. Must be a positive integer string.", Action = "UnStake-Failure" } })
      return
    end



    -- Check user's staked balance in that pool
    local current_stake = Stakers[user][pool_id] or '0'
    if not current_stake or BintUtils.lt(current_stake, amount_to_unstake) then
      Logger.warn("UnStake failed: Insufficient staked balance for user " ..
        user .. " in pool " .. pool_id .. ". Staked: " .. current_stake .. ", Requested: " .. amount_to_unstake)
      msg.reply({ Tags = { Code = "403", Error = "Insufficient staked balance.", Action = "UnStake-Failure" } })
      return
    end
    -- update pool stake amount
    Pools[pool_id].cur_staking = BintUtils.subtract(Pools[pool_id].cur_staking, amount_to_unstake)

    -- update User stake amount

    Stakers[user][pool_id] = BintUtils.subtract(Stakers[user][pool_id], amount_to_unstake)
    -- send apus back
    sendApus(user, amount_to_unstake, "UnStake APUS")
    -- Record unstaking transaction (updates current_stakes)
    PoolMgrDb:recordStakingTransaction(user, pool_id, 'UNSTAKE', amount_to_unstake)
    local new_stake_balance = Stakers[user][pool_id]
    Logger.info("UnStake successful for " ..
      user .. " from pool " .. pool_id .. ". APUS sent. New stake balance: " .. new_stake_balance)
    msg.reply({
      Tags = { Code = "200", Action = "UnStake-Success" },
      Data = json.encode({
        pool_id = pool_id,
        unstaked_amount =
            amount_to_unstake,
        new_stake_balance = new_stake_balance
      })
    })
    Send({
      device = 'patch@1.0',
      pools = getPools(),
      stakers = getStakers()
    })
  end
)

local function computeInterest(timestamp)
  for pool_id, pool in pairs(Pools) do
    if BintUtils.gt(pool.staking_start, timestamp) or BintUtils.lt(pool.staking_end,timestamp) then
      Logger.warn("Cuurent time is  " .. timestamp)
      Logger.warn("Pool " ..
        pool_id ..
        " not in staking period. Staking period: ")
    else
      -- 1. Get all eligible stake portions for the pool
      local total_effective_stake_amount = PoolMgrDb:getTotalEffectiveStakeAmountInPool(pool_id)
      if BintUtils.le(total_effective_stake_amount, '0') then
        Logger.info("Mgr-Distribute-Interest: No eligible stake portions found for pool " ..
          pool_id .. ". No interest will be distributed.")
        return
      end
      Logger.info("Mgr-Distribute-Interest: Total effective stake amount in pool " ..
        pool_id .. " is " .. total_effective_stake_amount)

      -- 2. Aggregate stakes per user and calculate total effective stake

      local eligible_stakers = PoolMgrDb:getEligibleStakersInPool(pool_id)
      local user_total_effective_stakes = {} -- Key: user_wallet_address, Value: bint total effective stake
      local overall_total_effective_stake = '0'
      for _, _staker in ipairs(eligible_stakers) do
        local user_addr = _staker.user_wallet_address
        local stake_amount = _staker.staked_amount

        overall_total_effective_stake = BintUtils.add(overall_total_effective_stake, stake_amount)
        if user_total_effective_stakes[user_addr] then
          user_total_effective_stakes[user_addr] = BintUtils.add(user_total_effective_stakes[user_addr], stake_amount)
        else
          user_total_effective_stakes[user_addr] = stake_amount
        end
      end
      -- 3. Calculate and record interest for each user, prepare batch transfer data

      local total_interest_calculated_for_distribution = '0' -- For tracking dust
      local daily_interest_amount = pool.rewards_amount
      local floor = math.floor
      for user_addr, user_effective_stake in pairs(user_total_effective_stakes) do
        -- Calculate user's interest: (user_effective_stake * total_daily_interest_amount) / overall_total_effective_stake
        Logger.info("User: " .. user_addr .. ", Effective Stake: " .. user_effective_stake)
        local numerator = BintUtils.multiply(user_effective_stake, daily_interest_amount)
        local user_interest_share = floor(BintUtils.divide(numerator, overall_total_effective_stake)) -- Integer division
        local user_interest_share_bint = BintUtils.toBalanceValue(user_interest_share)
        if BintUtils.gt(user_interest_share_bint, '0') then
          -- Add interst to undistribute interest
          Undistributed_Interest[user_addr] = BintUtils.add(Undistributed_Interest[user_addr] or '0',
            user_interest_share_bint)
          -- Record the distribution in the database
          local record_ok, record_err = PoolMgrDb:recordInterestDistribution(user_addr, pool_id, user_interest_share_bint,
            user_effective_stake)
          if not record_ok then
            Logger.error("Mgr-Distribute-Interest: Failed to record interest distribution for user " .. user_addr ..
              " in pool " ..
              pool_id .. ". Error: " .. (record_err or "Unknown") .. ". Skipping this user for batch transfer.")
            goto continue_loop -- Skip this user if DB recording fails
          end
          -- Add to batch transfer list
          -- table.insert(batch_transfer_data, user_addr .. "," .. user_interest_share)

          total_interest_calculated_for_distribution = BintUtils.add(total_interest_calculated_for_distribution,
            user_interest_share_bint)
          Logger.debug("Mgr-Distribute-Interest: User " .. user_addr .. " in pool " .. pool_id ..
            " eligible for " .. user_interest_share_bint .. " interest from stake " .. user_effective_stake)
        else
          Logger.debug("Mgr-Distribute-Interest: User " .. user_addr .. " in pool " .. pool_id ..
            " calculated interest share is zero or less. Stake: " .. user_effective_stake)
        end
        ::continue_loop::
      end
    end
  end
end
Handlers.add("Mgr-Distribute-Interest",
  Handlers.utils.hasMatchingTag("Action", "Cron"),
  function(msg)
    computeInterest(msg.Timestamp)
    local batch_transfer_data = {} -- Array of "address,amount" strings


    -- loop Undistributed_Interest and sum up all interst
    local total_undistributed_interest = '0'
    for user_addr, user_interest in pairs(Undistributed_Interest) do
      total_undistributed_interest = BintUtils.add(total_undistributed_interest, user_interest)
      -- Add to batch transfer list
      batch_transfer_data[#batch_transfer_data + 1] = user_addr .. "," .. user_interest

    end


    Logger.info("Total undistributed_interest: " .. total_undistributed_interest)
    Logger.info("Total Treasure Balance: " .. InterestFromTreasure)
    -- if Balance is not enough, exit
    if BintUtils.lt(InterestFromTreasure, total_undistributed_interest) then
      Logger.info("Mgr-Distribute-Interest: Insufficient APUS balance for interest distribution. Available: " ..
        InterestFromTreasure .. ", Required: " .. total_undistributed_interest)
      return
    end

    -- 4. Perform Batch Transfer if there's data
    if #batch_transfer_data > 0 then
      local csv_data_string = table.concat(batch_transfer_data, "\n")
      ao.send({ Target = ApusTokenId, Tags = { Cast = "true", Action = "Batch-Transfer" }, Data = csv_data_string })
      
      for user_addr, user_interest in pairs(Undistributed_Interest) do
      -- Add distributed interest
        Distributed_Interest[user_addr] = BintUtils.add(Distributed_Interest[user_addr] or '0',user_interest)
      end
      
      -- clean undistribute interest and deduct treasure balance
      InterestFromTreasure = BintUtils.subtract(InterestFromTreasure, total_undistributed_interest)
      Undistributed_Interest = {}
    else
      Logger.info("Mgr-Distribute-Interest: No users eligible for interest transfer after calculations  ")
    end
    
    Send({
      device = 'patch@1.0',
      distributed_interest = getDistributedInterest()
    })

    Logger.info("Mgr-Distribute-Interest: Interest distribution process completed ")
  end
)
-- Initialization flag to prevent re-initialization
Initialized = Initialized or false

if Initialized == false then
  Initialized = true
  local pool1 = createPool("1", "APUS_network", "5000000000000000000", "2054000000000000",
  "1748397808297", "1757390400000")
  Pools[pool1.pool_id] = pool1
  Send({
    device = 'patch@1.0',
    credits = getCredits(),
    pools = getPools(),
    stakers = getStakers(),
    process_info = getInfo(),
    distributed_interest = getDistributedInterest()
  })
end


