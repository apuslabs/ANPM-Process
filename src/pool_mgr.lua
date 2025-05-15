local BintUtils = require('utils.bint_utils')
local json = require('json')
local Logger = require('utils.log')
local PoolMgrDb = require('dao.pool_mgr_db').new() 
local Config = require('utils.config')

Undistributed_Credits = Undistributed_Credits or {}
Pools = Pools or {}
Stakers = Stakers or {}
CreditExchangeRate = CreditExchangeRate or Config.CreditExchangeRate


-- Constants from Config
ApusTokenId =  ApusTokenId or Config.ApusTokenId


local function isValidPool(poolId)
  return Pools[poolId] ~= nil
end
local function createPool(pool_id, creator, staking_capacity,rewards_amount,created_at,started_at)
  return {
    pool_id = pool_id,
    creator = creator,
    staking_capacity = staking_capacity,
    rewards_amount = rewards_amount,
    created_at = created_at,
    started_at = started_at,
    cur_staking = '0',
    apr = 0.05,
    name = "Qwen",
    description = "First Pool description",
    image_url = "https://qianwen-res.oss-accelerate-overseas.aliyuncs.com/qwen3-banner.png",
  }
end
function UpdatePool(pool_id,rewards_amount)
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
Logger.info('Pool Manager Process  Started. Owner12')

-- Credits handlers
Handlers.add(
  "Buy-Credit",
  function(msg) return (msg.Tags.Action == 'Credit-Notice') and (msg.Tags['X-AN-Reason'] == "Buy-Credit") end,
  function (msg)
    local user = msg.Tags.Sender -- The user who sent the APUS
    local apus_amount = msg.Tags.Quantity
    local ref = msg.Tags["X-Reference"] or msg.Tags.Reference
    Logger.info("Processing Buy-Credit from User: " .. user .. ", APUS Quantity: " .. apus_amount)
    -- assert Token is APUS 
    assert(msg.From == ApusTokenId, 'Invalid Token')
    -- Calculate credits to add
    
    local credits_to_add = BintUtils.multiply(apus_amount, CreditExchangeRate)
    Logger.info("User: " .. user .. ", Credits to Add: " .. credits_to_add .. ", Credit Exchange Rate: " .. CreditExchangeRate)
    Undistributed_Credits[user] = BintUtils.add(Undistributed_Credits[user] or '0', credits_to_add)
    -- Record transaction and update balance (adds to unallocated pool '0')
    PoolMgrDb:recordCreditTransaction(ref,user, "buy", apus_amount, "0")
    msg.reply({
      Tags = { Code = "200" },
      Data = json.encode({ user = user, balance = BintUtils.toBalanceValue(Undistributed_Credits[user] or '0') })
    })
  end
)

Handlers.add(
  "Get-Undistributed-Credits",
  Handlers.utils.hasMatchingTag("Action", "Get-Undistributed-Credits"),
  function(msg)
    local user = msg.Tags.Recipient or msg.From 
    msg.reply({
      Tags = { Code = "200" },
      Data = json.encode({ user = user, balance = BintUtils.toBalanceValue(Undistributed_Credits[user] or '0') })
    })
  end
)

Handlers.add(
  "Get-All-Credits",
  Handlers.utils.hasMatchingTag("Action", "Get-All-Credits"),
  function(msg)
    -- Permission Check
    Logger.info("get ALL credits By " .. msg.From)
    if msg.From ~= Owner then
      Logger.warn(" Get-All-Credits denied: Sender " .. msg.From .. " is not the owner.")
      msg.reply({Tags = { Code = "403"} , Data = "Unauthorized"})
      return
    end
    msg.reply({
      Tags = { Code = "200" },
      Data = json.encode(Undistributed_Credits)
    })
  end
)

--- Handler: Get-Pool-List
-- Description: Returns All existing pools.
-- Pattern: { Action = "Get-Pool-List" }


Handlers.add(
  "Get-Pool-List",
  Handlers.utils.hasMatchingTag("Action", "Get-Pool-List"),
  function(msg)
    -- calculate current APY for each pool
    for pool_id, pool in pairs(Pools) do
      local total_staking = Pools[pool_id].cur_staking
      local total_rewards = Pools[pool_id].rewards_amount
      local apr = 0.05*365
      pool.apr = apr
    end
    msg.reply({
      Tags = { Code = "200" },
      Data = json.encode({ pools = Pools})
    })
  end
)

--- Handler: Get-Credits-Records
-- Description: Returns Credits records for a specific user.
-- Pattern: { Action = "Get-Credits-Records" }
Handlers.add(
  "Get-Credits-Records",
  Handlers.utils.hasMatchingTag("Action", "Get-Credits-Records"),
  function(msg)
    local user = msg.Tags.Recipient or msg.From 
    Logger.info("get credits records for " .. user)
    local records = PoolMgrDb:getUserCreditsRecords(user)
    local records_list = {}
    for _, record in pairs(records) do
      records_list[#records_list+1] = { ref = record.ref, record = record }
    end
    msg.reply({
      Tags = { Code = "200" },
      Data = json.encode(records_list)
    })
  end
)

--- Handler: Get-All-Credits-Records
-- Description: Returns  All Credits records.
-- Pattern: { Action = "Get-All-Credits-Records" }
Handlers.add(
  "Get-All-Credits-Records",
  Handlers.utils.hasMatchingTag("Action", "Get-All-Credits-Records"),
  function(msg)
    -- Permission Check
    Logger.info("get ALL credits Records By " .. msg.From)
    if msg.From ~= Owner then
      Logger.warn(" Get-All-Credits-Records denied: Sender " .. msg.From .. " is not the owner.")
      msg.reply({Tags = { Code = "403"} , Data = "Unauthorized"})
      return
    end
    local records = PoolMgrDb:getAllCreditsRecords()
    local records_list = {}
    for _, record in pairs(records) do
      records_list[#records_list+1] = { ref = record.ref, record = record }
    end
    msg.reply({
      Tags = { Code = "200" },
      Data = json.encode(records_list)
    })
  end
)

--- Handler: Info
-- Description: Returns basic info about the Pool Manager.
-- Pattern: { Action = "Info" }
Handlers.add(
  "Info",
  Handlers.utils.hasMatchingTag("Action", "Info"),
  function (msg)
      Logger.info("Request for Pool Manager info from " .. msg.From)
      local info = {
          process_id = ao.id,
          owner = Owner,
          apus_token = ApusTokenId,
          credit_exchange_rate = CreditExchangeRate,
          -- Add other relevant info
      }
      msg.reply({ Data = json.encode(info) })
  end
)


--- Handler: Transfer-Credits
-- Description: User  transfer their  credits from a specific pool.
-- Pattern: { Action = "AN-Credit-Notice" }
-- Message Data: { User = "...", Quantity = "..." }
Handlers.add(
  "Transfer-Credits",
  Handlers.utils.hasMatchingTag("Action", "AN-Credit-Notice"),
  function (msg)
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
    PoolMgrDb:recordCreditTransaction(ref,user, "transfer", quantity, pool_id)
    -- Send confirmation back to Pool
    msg.reply({ Tags = { Code = "200" } })
  end
)

--- Handler: Add-Credit
-- Description: User  transfer their  credits to  a specific pool.
-- Pattern: { Action = "AN-Credit-Notice" }


Handlers.add(
  "Add-Credit",
  Handlers.utils.hasMatchingTag("Action", "Add-Credit"),
  function (msg)
    local user = msg.From
    local quantity = msg.Tags.Quantity
    local pool_id = msg.Tags.PoolId
    local ref = msg.Tags["X-Reference"] or msg.Tags.Reference
    -- Validate pool_id and quantity
    assert(type(user) == 'string', 'user is required!')
    assert(type(quantity) == 'string', 'Quantity is required!')
    assert(BintUtils.gt(quantity, '0'), 'Quantity must be greater than 0')
    -- Check if the balance is sufficient
    local pool_balance = Undistributed_Credits[user]
    if BintUtils.lt(pool_balance, quantity) then
      Logger.warn("Add-Credit failed: Insufficient pool balance for pool " .. pool_id .. ". Balance: " .. pool_balance .. ", Requested: " .. quantity)
      msg.reply({ Tags = { Code = "403", Error = "Insufficient pool balance." }})
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
    -- Send notification to the target Pool process
    ao.send({
        Target = pool_id,
        Action = "AN-Credit-Notice", -- As defined in Pool LLD
        From = ao.id, -- Send from this Pool Mgr process
        User = user,
        Quantity = quantity
    })

    PoolMgrDb:recordCreditTransaction(ref,user, "add", quantity, pool_id)
    -- Send confirmation back to user
    msg.reply({ Tags = { Code = "200" },  
      Data = json.encode({ user = user, balance = BintUtils.toBalanceValue(Undistributed_Credits[user] or '0') }) })
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
  function(msg) return (msg.Tags.Action == 'Credit-Notice') and (msg.Tags['X-AN-Reason'] == "Stake") end,
  function (msg)

    local pool_id = msg.Tags["X-PoolId"]
    local user = msg.Tags.Sender -- The user who sent the APUS
    local apus_amount = msg.Tags.Quantity


    assert(type(pool_id) == 'string', 'Missing X-PoolId')
    assert(msg.From == ApusTokenId, 'Invalid Token')
    -- Check if pool exists and get capacity
    local pool = Pools[pool_id]
    if not pool then
        Logger.error("Stake failed: Pool " .. pool_id .. " not found.")
        ao.send({ Target = user, Tags = { Code = "404", Error = "Target pool for staking not found.", Action="Stake-Failure" }})
        return
    end
    Logger.info("Processing Stake from User: " .. user .. ", APUS Quantity: " .. apus_amount .. ", Pool: " .. pool_id)

    -- Check staking capacity
    local current_pool_stake = Pools[pool_id].cur_staking
    Logger.info("Current Pool Stake: " .. current_pool_stake)
    local capacity = pool.staking_capacity
    local potential_new_total = BintUtils.add(current_pool_stake, apus_amount)


    if BintUtils.gt(potential_new_total, capacity) then
        Logger.error("Stake failed: Staking amount " .. apus_amount .. " exceeds pool " .. pool_id .. " capacity (" .. capacity .. "). Current stake: " .. current_pool_stake)
        sendApus(user, apus_amount, "Refund: Stake Exceeds Pool Capacity")
        ao.send({ Target = user, Tags = { Code = "403", Error = "Staking amount exceeds pool capacity.", Action="Stake-Failure" }})
        return
    end
    
    -- Add pool cur_staking
    Pools[pool_id].cur_staking = potential_new_total
    Logger.info("here " .. current_pool_stake)
    -- Add user staking amount
    if Stakers[user] == nil then
      Stakers[user] = {}
    end
    Stakers[user][pool_id] = BintUtils.add(Stakers[user][pool_id] or '0',apus_amount)
    -- Record staking transaction
    PoolMgrDb:recordStakingTransaction(user, pool_id, 'STAKE', apus_amount)

    local new_stake_balance = Stakers[user][pool_id]
    Logger.info("Stake successful for " .. user .. " in pool " .. pool_id .. ". New stake balance: " .. new_stake_balance)
    ao.send({ Target = user, Tags = { Code = "200", Action = "Stake-Success" }, Data = json.encode({ pool_id = pool_id, staked_amount = apus_amount, new_stake_balance = new_stake_balance }) })
  end
)

Handlers.add(
  "Mgr-UnStake",
  Handlers.utils.hasMatchingTag("Action", "UnStake"),
  function (msg)
    local user = msg.From
    local pool_id = msg.Tags.PoolId
    local amount_to_unstake = msg.Tags.Quantity
    local pool = Pools[pool_id]
    -- Validate input
    if not pool_id or type(pool_id) ~= "string" or not pool then
       Logger.error("UnStake failed: Invalid pool_id from " .. user)
       msg.reply({ Tags = { Code = "400", Error = "Invalid pool_id provided.", Action="UnStake-Failure" }})
       return
    end
    if not amount_to_unstake or BintUtils.le(amount_to_unstake, '0') then
       Logger.error("UnStake failed: Invalid amount from " .. user)
       msg.reply({ Tags = { Code = "400", Error = "Invalid amount provided. Must be a positive integer string.", Action="UnStake-Failure" }})
       return
    end



    -- Check user's staked balance in that pool
    local current_stake = Stakers[user][pool_id] or '0'
    if not current_stake or BintUtils.lt(current_stake, amount_to_unstake) then
        Logger.warn("UnStake failed: Insufficient staked balance for user " .. user .. " in pool " .. pool_id .. ". Staked: " .. current_stake .. ", Requested: " .. amount_to_unstake)
        msg.reply({ Tags = { Code = "403", Error = "Insufficient staked balance.", Action="UnStake-Failure" }})
        return
    end
    -- update pool stake amount
    Pools[pool_id].cur_staking = BintUtils.subtract(Pools[pool_id].cur_staking ,amount_to_unstake)
    
    -- update User stake amount
    
    Stakers[user][pool_id] = BintUtils.subtract(Stakers[user][pool_id], amount_to_unstake)
    -- send apus back
    sendApus(user, amount_to_unstake, "UnStake APUS")
    -- Record unstaking transaction (updates current_stakes)
    PoolMgrDb:recordStakingTransaction(user, pool_id, 'UNSTAKE', amount_to_unstake)
    local new_stake_balance = Stakers[user][pool_id]
    Logger.info("UnStake successful for " .. user .. " from pool " .. pool_id .. ". APUS sent. New stake balance: " .. new_stake_balance)
    msg.reply({ Tags = { Code = "200", Action = "UnStake-Success" }, Data = json.encode({ pool_id = pool_id, unstaked_amount = amount_to_unstake, new_stake_balance = new_stake_balance }) })
  end
)

--- Handler: Get-Staking
-- Description: Returns the current staking amount for a user in a specific pool.
-- Pattern: { Action = "Get-Staking" }
-- Message Data: { pool_id = "..." }
Handlers.add(
  "Mgr-Get-Staking",
  Handlers.utils.hasMatchingTag("Action", "Get-Staking"),
  function (msg)
    local user = msg.Tags.Recipient or msg.From 
    local pool_id = msg.Tags.PoolId
    
    -- Validate input
    if not pool_id or type(pool_id) ~= "string" or not isValidPool(pool_id) then
      Logger.error("Get-Staking failed: Invalid pool_id from " .. user)
      msg.reply({ Tags = { Code = "400", Error = "Invalid pool_id provided."}})
      return
    end
    

    -- Get user's staked balance in the specified pool
    local current_stake = Stakers[user][pool_id] or '0'
    
    msg.reply({ 
      Tags = { Code = "200"}, 
      Data = json.encode({ 
        pool_id = pool_id, 
        current_stake = current_stake,
      })
    })
  end
)

--- Handler: Get-Pool-Staking
-- Description: Returns the total staked amount in a specific pool.
-- Pattern: { Action = "Get-Pool-Staking" }
-- Message Data: { pool_id = "..." }
Handlers.add(
  "Mgr-Get-Pool-Staking",
  Handlers.utils.hasMatchingTag("Action", "Get-Pool-Staking"),
  function (msg)
    local pool_id = msg.Tags.PoolId
    local pool = Pools[pool_id]
    -- Validate input
    if not pool_id or type(pool_id) ~= "string" or not isValidPool(pool_id) then
      Logger.error("Get-Pool-Staking failed: Invalid pool_id from " .. msg.From)
      msg.reply({ Tags = { Code = "400", Error = "Invalid pool_id provided.", Action="Get-Pool-Staking-Failure" }})
      return
    end
    
    -- Get total staked amount in the pool
    local total_pool_stake = pool.cur_staking or '0'
    Logger.info("Get-Pool-Staking: Total staked in pool " .. pool_id .. " is " .. total_pool_stake)
    
    msg.reply({ 
      Tags = { Code = "200", Action = "Get-Pool-Staking-Success" }, 
      Data = json.encode({ 
        pool_id = pool_id, 
        total_stake = total_pool_stake,
        capacity = pool.staking_capacity
      })
    })
  end
)

--- Handler: Get-All-Staking
-- Description: Returns all staking records (for internal use/backup).
-- Pattern: { Action = "Get-All-Staking" }
Handlers.add(
  "Mgr-Get-All-Staking",
  Handlers.utils.hasMatchingTag("Action", "Get-All-Staking"),
  function (msg) 
    -- Get all staking transactions
    local records = PoolMgrDb:getAllStakeRecords()
    if not records then
      records = {}
    end
    
    Logger.info("Get-All-Staking: Returning " .. #records .. " staking records")
    msg.reply({
      Tags = { Code = "200", Action = "Get-All-Staking-Success" },
      Data = json.encode(records)
    })
  end
)



-- Initialization flag to prevent re-initialization
Initialized = Initialized or false
-- Immediately Invoked Function Expression (IIFE) for initialization logic
(function()
  if Initialized == false then
    Initialized = true
  else
    print("Already Initialized. Skip Initialization.")
    return
  end
  print("Initializing ...")
  local pool1 = createPool("1","Alex","20000000000000000","100000000","Today","NextDay")
  Pools[pool1.pool_id] = pool1
  assert(next(Pools) ~= nil, "Initiali First pool failed")
end)()