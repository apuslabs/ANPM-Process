local BintUtils = require('utils.bint_utils')
local json = require('json')
local Logger = require('utils.log')
local PoolMgrDb = require('dao.pool_mgr_db').new() -- Initialize DAL
Undistributed_Credits = Undistributed_Credits or {}
Pools = Pools or {}
CreditExchangeRate = 1
ApusTokenId = ApusTokenId or "1uES191BAwwSaTBviqtexLpDhRu_zBc0ewHL2gIA1yo"
--Owner = "aK2fFFBJ1Hilzg_3yeoJuFywZJ8JJXhxMVC3WjBmPkE"
-- Constants from Config
LogLevel = LogLevel or 'info'


local PoolTemplate = {
  pool_id = nil,
  creator = nil,
  staking_capacity = 0,
  rewards_amount = 0,
  created_at = nil,
  started_at = nil
}

local function createPool(pool_id, creator, staking_capacity,rewards_amount,created_at,started_at)
  PoolTemplate.pool_id = pool_id
  PoolTemplate.creator = creator
  PoolTemplate.staking_capacity = staking_capacity
  PoolTemplate.rewards_amount = rewards_amount
  PoolTemplate.created_at = created_at
  PoolTemplate.started_at = started_at
  return PoolTemplate
end

Logger.info('Pool Manager Process  Started. Owner999')

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
      Data = json.encode({ User = user, Balance = BintUtils.toBalanceValue(Undistributed_Credits[user] or '0') })
    })
  end
)
local function isValidPool(poolId)
  return Pools[poolId] ~= nil
end
Handlers.add(
  "Get-Undistributed-Credits",
  Handlers.utils.hasMatchingTag("Action", "Get-Undistributed-Credits"),
  function(msg)
    local user = msg.From
    msg.reply({
      Tags = { Code = "200" },
      Data = json.encode({ User = user, Balance = BintUtils.toBalanceValue(Undistributed_Credits[user] or '0') })
    })
  end
)
Handlers.add(
  "Set-Credit-Ratio",
  Handlers.utils.hasMatchingTag("Action", "Set-Credit-Ratio"),
  function (msg)
    -- Permission Check
    if msg.From ~= Owner then
      Logger.warn("Set-Credit-Ratio denied: Sender " .. msg.From .. " is not the owner.")
      msg.reply({ Tags = { Code = "403" } , Data = "Unauthorized" })
      return
    end

    local new_rate = msg.Tags.Ratio
    if not new_rate or  BintUtils.lt(new_rate, '0') then
       Logger.error("Set-Credit-Ratio failed: Invalid rate amount provided: " .. tostring(new_rate))
       msg.reply({Tags = { Code = "400" }, Data = "Invalid rate amount" })
       return
    end

    CreditExchangeRate = new_rate
    Logger.info("Credit exchange rate updated to: " .. CreditExchangeRate .. " by owner.")
    msg.reply({Tags = { Code = "200" }, Data = "CreditExchangeRate set to: " .. CreditExchangeRate .. " by owner." })
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
--- Handler: Get-Credits-Records
-- Description: Returns Credits records for a specific user.
-- Pattern: { Action = "Get-Credits-Records" }
Handlers.add(
  "Get-Credits-Records",
  Handlers.utils.hasMatchingTag("Action", "Get-Credits-Records"),
  function(msg)
    Logger.info("get credits records for " .. msg.From)
    local records = PoolMgrDb:getUserCreditsRecords(msg.From)
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
-- Description: User  transfer their  credits to a specific pool.
-- Pattern: { Action = "Transfer-Credits" }
-- Message Data: { pool_id = "...", amount = "..." }
Handlers.add(
  "Transfer-Credits",
  Handlers.utils.hasMatchingTag("Action", "Transfer-Credits"),
  function (msg)
    local sender = msg.Sender
    local quantity = msg.Tags.Quantity
    local pool_id = msg.From
    local ref = msg.Tags["X-Reference"] or msg.Tags.Reference
    -- Validate pool_id and quantity
    assert(type(sender) == 'string', 'user is required!')
    assert(type(quantity) == 'string', 'Quantity is required!')
    assert(BintUtils.gt(quantity, '0'), 'Quantity must be greater than 0')
    -- Check if the pool exists
    if not isValidPool(pool_id) then
      Logger.error("Allocate-Credits failed: Invalid PoolId " .. pool_id)
      msg.reply({ Tags = { Code = "400" }, Data = "Invalid PoolId" })
      return
    end
    local pool_balance = Undistributed_Credits[user]
    Logger.info("Processing credit transfer for " .. sender .. " from pool " .. pool_id .. ", Amount: " .. quantity)

    -- TODO Add records into Database
    
    Undistributed_Credits[user] = BintUtils.add(pool_balance, quantity)
    PoolMgrDb:recordCreditTransaction(ref,sender, "transfer", quantity, pool_id)
    -- Send confirmation back to Pool
    msg.reply({ Tags = { Code = "200" }, Data = json.encode({ Credits = Undistributed_Credits[user] }) })
  end
)

--- Handler: Add-Credit
-- Description: User  transfer their  credits to from  specific pool.
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
    msg.reply({ Tags = { Code = "200" }, Data = json.encode({ Credits = Undistributed_Credits[user] }) })
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
  "Buy-Apus",
  function(msg) return (msg.Tags.Action == 'Credit-Notice') and (msg.Tags['X-AN-Reason'] == "Buy-Apus") end,
  function (msg)

    local pool_id = msg.Tags["X-AN-Pool-Id"]
    local user = msg.Tags.Sender -- The user who sent the APUS
    local apus_amount = msg.Tags.Quantity
    Logger.info("Processing Stake from User: " .. user .. ", APUS Quantity: " .. apus_amount .. ", Pool: " .. pool_id)

    assert(pool_id, 'Missing X-AN-Pool-Id')
    -- Check if pool exists and get capacity
    --local pool_info = PoolMgrDb:getPool(pool_id) // ToDO:
    local pool_info = nil
    pool_info = {
        id = pool_id,
        name = "Default Test Pool",
        owner = user,
        staking_capacity = "1000000000000", -- 1 million APUS
        status = "active",
        created_at = os.time()
    }
    if not pool_info then
        Logger.error("Stake failed: Pool " .. pool_id .. " not found.")
        ao.send({ Target = user, Tags = { Code = "404", Error = "Target pool for staking not found.", Action="Stake-Failure" }})
        return
    end

    Logger.info("Pool Info: " .. json.encode(pool_info))

    -- Check staking capacity
    local current_pool_stake = PoolMgrDb:getTotalPoolStake(pool_id)
    Logger.info("Current Pool Stake: " .. current_pool_stake)
    local capacity = pool_info.staking_capacity
    local potential_new_total = BintUtils.add(current_pool_stake, apus_amount)


    if BintUtils.gt(potential_new_total, capacity) then
        Logger.error("Stake failed: Staking amount " .. apus_amount .. " exceeds pool " .. pool_id .. " capacity (" .. capacity .. "). Current stake: " .. current_pool_stake)
        sendApus(user, apus_amount, "Refund: Stake Exceeds Pool Capacity")
        ao.send({ Target = user, Tags = { Code = "403", Error = "Staking amount exceeds pool capacity.", Action="Stake-Failure" }})
        return
    end
    
    -- Record staking transaction
    PoolMgrDb:recordStakingTransaction(user, pool_id, 'STAKE', apus_amount)

    local new_stake_balance = PoolMgrDb:getCurrentStake(user, pool_id)
    Logger.info("Stake successful for " .. user .. " in pool " .. pool_id .. ". New stake balance: " .. new_stake_balance)
    ao.send({ Target = user, Tags = { Code = "200", Action = "Stake-Success" }, Data = json.encode({ pool_id = pool_id, staked_amount = apus_amount, new_stake_balance = new_stake_balance }) })
  end
)

--- Handler: Stake (via APUS Transfer)
-- Description: Handles incoming APUS transfers intended for staking in a specific pool.
-- Pattern: { Action = "Credit-Notice", From = "<APUS Token ID>", ['X-AN-Reason'] = "Stake", ['X-AN-Pool-Id'] = "<pool_id>" }
--function(msg) return (msg.Action == "Credit-Notice") and (msg.From == ApusTokenId) and (msg.Tags['X-AN-Reason'] == 'Stake') end,
-- Handlers.add(
--   "Mgr-Stake",
--   function(msg) return (msg.Tags.Action == 'Credit-Notice') and (msg.Tags['X-AN-Reason'] == "Stake") end,
--   function (msg)
--     local user = msg.Sender
--     local apus_amount = msg.Quantity
--     local pool_id = msg.Tags["Pool-Id"]

--     if not pool_id then
--        Logger.error("Invalid Stake notice: Missing X-AN-Pool-Id tag. Msg: " .. json.encode(msg))
--        sendApus(user, apus_amount, "Refund: Stake Pool ID Missing")
--        return
--     end

--     Logger.log("Processing Stake from User: " .. user .. ", APUS Quantity: " .. apus_amount .. ", Pool: " .. pool_id)

--     -- Check if pool exists and get capacity
--     local pool_info = PoolMgrDb:getPool(pool_id)
--     pool_info = pool_info or {
--         id = pool_id,
--         name = "Default Test Pool",
--         owner = user,
--         staking_capacity = "1000000000000", -- 1 million APUS
--         status = "active",
--         created_at = os.time()
--     }
--     if not pool_info then
--         Logger.error("Stake failed: Pool " .. pool_id .. " not found.")
--         sendApus(user, apus_amount, "Refund: Stake Pool Not Found")
--         ao.send({ Target = user, Tags = { Code = "404", Error = "Target pool for staking not found.", Action="Stake-Failure" }})
--         return
--     end

--     -- Check staking capacity
--     local current_pool_stake = PoolMgrDb:getTotalPoolStake(pool_id)
--     local capacity = pool_info.staking_capacity
--     local potential_new_total = BintUtils.add(current_pool_stake, apus_amount)

--     if BintUtils.gt(potential_new_total, capacity) then
--         Logger.warn("Stake failed: Staking amount " .. apus_amount .. " exceeds pool " .. pool_id .. " capacity (" .. capacity .. "). Current stake: " .. current_pool_stake)
--         sendApus(user, apus_amount, "Refund: Stake Exceeds Pool Capacity")
--         ao.send({ Target = user, Tags = { Code = "403", Error = "Staking amount exceeds pool capacity.", Action="Stake-Failure" }})
--         return
--     end

--     -- Record staking transaction
--     PoolMgrDb:recordStakingTransaction(user, pool_id, 'STAKE', apus_amount)

--     local new_stake_balance = PoolMgrDb:getCurrentStake(user, pool_id)
--     Logger.log("Stake successful for " .. user .. " in pool " .. pool_id .. ". New stake balance: " .. new_stake_balance)
--     ao.send({ Target = user, Tags = { Code = "200", Action = "Stake-Success" }, Data = json.encode({ pool_id = pool_id, staked_amount = apus_amount, new_stake_balance = new_stake_balance }) })
--     msg.reply({
--       Tags = { Code = "200" }
--     })

--   end
-- )

--- Handler: UnStake
-- Description: Allows a user to unstake APUS from a pool. Sends APUS back to the user.
-- Pattern: { Action = "UnStake" }
-- Message Data: { pool_id = "...", amount = "..." }

Handlers.add(
  "Mgr-UnStake",
  Handlers.utils.hasMatchingTag("Action", "UnStake"),
  function (msg)
    local user = msg.From
    local pool_id = msg.Data.pool_id
    local amount_to_unstake = msg.Data.amount

    -- Validate input
    if not pool_id or type(pool_id) ~= "string" or pool_id == "" then
       Logger.error("UnStake failed: Invalid pool_id from " .. user)
       msg.reply({ Tags = { Code = "400", Error = "Invalid pool_id provided.", Action="UnStake-Failure" }})
       return
    end
    if not amount_to_unstake or BintUtils.le(amount_to_unstake, '0') then
       Logger.error("UnStake failed: Invalid amount from " .. user)
       msg.reply({ Tags = { Code = "400", Error = "Invalid amount provided. Must be a positive integer string.", Action="UnStake-Failure" }})
       return
    end

     -- Check if pool exists (sanity check)
    --local pool_info = PoolMgrDb:getPool(pool_id)
    local pool_info = nil
    pool_info = {
        id = pool_id,
        name = "Default Test Pool",
        owner = user,
        staking_capacity = "1000000000000", -- 1 million APUS
        status = "active",
        created_at = os.time()
    }
    if not pool_info then
        Logger.error("UnStake failed: Pool " .. pool_id .. " not found.")
        msg.reply({Tags = { Code = "404", Error = "Target pool for unstaking not found.", Action="UnStake-Failure" }})
        return
    end

    -- Check user's staked balance in that pool
    local current_stake = PoolMgrDb:getCurrentStake(user, pool_id)
    if BintUtils.lt(current_stake, amount_to_unstake) then
        Logger.warn("UnStake failed: Insufficient staked balance for user " .. user .. " in pool " .. pool_id .. ". Staked: " .. current_stake .. ", Requested: " .. amount_to_unstake)
        msg.reply({ Tags = { Code = "403", Error = "Insufficient staked balance.", Action="UnStake-Failure" }})
        return
    end

    Logger.info("Processing UnStake for " .. user .. " from pool " .. pool_id .. ", Amount: " .. amount_to_unstake)

    -- Record unstaking transaction (updates current_stakes)
    PoolMgrDb:recordStakingTransaction(user, pool_id, 'UNSTAKE', amount_to_unstake)
    local new_stake_balance = PoolMgrDb:getCurrentStake(user, pool_id)
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
    local user = msg.From
    local pool_id = msg.Data.pool_id
    
    -- Validate input
    if not pool_id or type(pool_id) ~= "string" or pool_id == "" then
      Logger.error("Get-Staking failed: Invalid pool_id from " .. user)
      msg.reply({ Tags = { Code = "400", Error = "Invalid pool_id provided."}})
      return
    end
    

    -- Get user's staked balance in the specified pool
    local current_stake = PoolMgrDb:getCurrentStake(user, pool_id)
    Logger.info("Get-Staking: User " .. user .. " has " .. current_stake .. " staked in pool " .. pool_id)
    
    -- Get user's total staked balance across all pools
    local total_stake = PoolMgrDb:getTotalUserStake(user)
    
    msg.reply({ 
      Tags = { Code = "200"}, 
      Data = json.encode({ 
        pool_id = pool_id, 
        current_stake = current_stake,
        total_stake = total_stake
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
    local pool_id = msg.Data.pool_id
    
    -- Validate input
    if not pool_id or type(pool_id) ~= "string" or pool_id == "" then
      Logger.error("Get-Pool-Staking failed: Invalid pool_id from " .. msg.From)
      msg.reply({ Tags = { Code = "400", Error = "Invalid pool_id provided.", Action="Get-Pool-Staking-Failure" }})
      return
    end
    
    -- Check if pool exists
    local pool_info = PoolMgrDb:getPool(pool_id)
    if not pool_info then
      Logger.error("Get-Pool-Staking failed: Pool " .. pool_id .. " not found.")
      msg.reply({Tags = { Code = "404", Error = "Pool not found.", Action="Get-Pool-Staking-Failure" }})
      return
    end
    
    -- Get total staked amount in the pool
    local total_pool_stake = PoolMgrDb:getTotalPoolStake(pool_id)
    Logger.info("Get-Pool-Staking: Total staked in pool " .. pool_id .. " is " .. total_pool_stake)
    
    msg.reply({ 
      Tags = { Code = "200", Action = "Get-Pool-Staking-Success" }, 
      Data = json.encode({ 
        pool_id = pool_id, 
        total_stake = total_pool_stake,
        capacity = pool_info.staking_capacity
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
    -- Permission Check
    if msg.From ~= Owner then
      Logger.warn("Get-All-Staking denied: Sender " .. msg.From .. " is not the owner.")
      msg.reply({Tags = { Code = "403"}, Data = "Unauthorized"})
      return
    end
    
    -- Get all staking transactions
    local sql = "SELECT * FROM user_staking_transactions ORDER BY created_at ASC;"
    local records = PoolMgrDb.dbAdmin:select(sql)
    
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

  -- check if the sum is 8% of the total supply
  local pool1 = createPool("1","Alex",20000,1000,"Today","NextDay")
  Pools[pool1.pool_id] = pool1
  assert(next(Pools) ~= nil, "Initiali First pool failed")
end)()