-- Pool Manager Process Implementation
-- Manages credits, staking, incentives, and pool registration.

-- AO Library (Implicitly available)
-- local ao = require('ao')

-- Utilities
local Logger = require('utils.log')
local BintUtils = require('utils.bint_utils')
local Permissions = require('utils.permissions')
local PoolMgrDb = require('pool_mgr.pool_mgr_db').new() -- Initialize DAL
local Config = require('pool_mgr.config')
local json = require('json') -- Assuming json library is available in AO env

-- Process State (In-memory, persisted via AO mechanisms)
-- Credit Exchange Rate: How many credits per 1 smallest unit of APUS token
local CreditExchangeRate = CreditExchangeRate or Config.CreditsPerApus -- Load from state or use config default

-- Constants from Config
local Owner = Config.Owner
local ApusTokenId = Config.ApusTokenId -- Needed for transfers

Logger.log('Pool Manager Process (' .. ao.id .. ') Started. Owner: ' .. Owner .. ', APUS Token: ' .. ApusTokenId)

-- ================= Helper Functions =================

-- Sends APUS tokens from this process to a recipient
local function sendApus(recipient, amount, reason)
    if BintUtils.le(amount, '0') then
        Logger.warn("sendApus: Attempted to send non-positive amount: " .. amount)
        return
    end
    Logger.log("Sending " .. amount .. " APUS to " .. recipient .. " for reason: " .. reason)
    ao.send({
        Target = ApusTokenId,
        Action = "Transfer",
        Recipient = recipient,
        Quantity = amount,
        ['X-AN-Reason'] = reason -- Add reason for context
    })
end

-- ================= Credit Handlers =================

--- Handler: Buy-Credit (via APUS Transfer)
-- Description: Handles incoming APUS transfers intended for buying credits.
-- Pattern: { Action = "Credit-Notice", From = "<APUS Token ID>", ['X-AN-Reason'] = "Buy-Credit" }
-- Note: Assumes APUS token sends 'Credit-Notice' on successful transfer.
Handlers.add(
  "Mgr-Buy-Credit",
  function(msg)
    -- Check if it's a credit notice from the correct token with the right reason
    return msg.Action == "Credit-Notice" and
           msg.From == ApusTokenId and
           msg.Tags['X-AN-Reason'] == 'Buy-Credit' and
           msg.Sender -- Sender is the original user who initiated the transfer
  end,
  function (msg)
    local user = msg.Sender -- The user who sent the APUS
    local apus_amount = msg.Quantity

    if not user or not apus_amount or not BintUtils.is_valid(apus_amount) or BintUtils.le(apus_amount, '0') then
      Logger.error("Invalid Buy-Credit notice: Missing Sender, Quantity, or invalid Quantity. Msg: " .. json.encode(msg))
      -- Cannot easily refund here as the APUS is already received. Log error.
      return
    end

    Logger.log("Processing Buy-Credit from User: " .. user .. ", APUS Quantity: " .. apus_amount)

    -- Calculate credits to add
    local credits_to_add = BintUtils.mul(apus_amount, CreditExchangeRate)
    Logger.log("Calculated credits to add: " .. credits_to_add .. " (Rate: " .. CreditExchangeRate .. ")")

    -- Record transaction and update balance (adds to unallocated pool '0')
    local success = PoolMgrDb:recordCreditTransaction(user, 'buy', credits_to_add, '0')

    if success then
      local new_balance = PoolMgrDb:getCurrentCredits(user, '0')
      Logger.log("Credits purchased successfully for " .. user .. ". New unallocated balance: " .. new_balance)
      -- Notify user of successful purchase
      ao.send({ Target = user, Tags = { Code = "200", Action = "Credits-Purchased" }, Data = json.encode({ credits_added = credits_to_add, new_unallocated_balance = new_balance }) })
    else
      Logger.error("Failed to record credit purchase transaction for " .. user)
      -- Critical error: APUS received but credits not granted. Needs manual intervention or alerting.
      -- Maybe try to refund APUS?
      sendApus(user, apus_amount, "Refund: Buy-Credit DB Error")
      ao.send({ Target = user, Tags = { Code = "500", Error = "Failed to process credit purchase, APUS refunded." }})
    end
  end
)

--- Handler: Set-Credit-Ratio
-- Description: Sets the APUS to Credits exchange ratio (Owner only).
-- Pattern: { Action = "Set-Credit-Ratio", From = "<Owner>" }
-- Message Data: { amount = "<new_rate>" } (Credits per 1 APUS unit)
Handlers.add(
  "Mgr-Set-Credit-Ratio",
  Handlers.utils.hasMatchingTag("Action", "Set-Credit-Ratio"),
  function (msg)
    -- Permission Check
    if not Permissions.is_owner(msg) then
      Logger.warn("Set-Credit-Ratio denied: Sender " .. msg.From .. " is not the owner.")
      ao.send({ Target = msg.From, Tags = { Code = "403", Error = "Unauthorized" }})
      return
    end

    local new_rate = msg.Data.amount
    if not new_rate or not BintUtils.is_valid(new_rate) or BintUtils.lt(new_rate, '0') then
       Logger.error("Set-Credit-Ratio failed: Invalid rate amount provided: " .. tostring(new_rate))
       ao.send({ Target = msg.From, Tags = { Code = "400", Error = "Invalid rate amount provided. Must be a non-negative integer string." }})
       return
    end

    CreditExchangeRate = new_rate
    Logger.log("Credit exchange rate updated to: " .. CreditExchangeRate .. " by owner.")
    ao.send({ Target = msg.From, Tags = { Code = "200", Action = "Credit-Ratio-Set" }, Data = CreditExchangeRate })
  end
)

--- Handler: Query-Credits
-- Description: Allows a user to query their credit balances (unallocated and per-pool).
-- Pattern: { Action = "Query-Credit" }
Handlers.add(
  "Mgr-Query-Credits",
  Handlers.utils.hasMatchingTag("Action", "Query-Credit"),
  function (msg)
    local user = msg.From
    Logger.log("User " .. user .. " requested their credit balances.")
    local user_credits = PoolMgrDb:getAllUserCredits(user)
    ao.send({ Target = user, Data = json.encode(user_credits) })
  end
)

--- Handler: Query-All-Credits (Owner Only)
-- Description: Returns all credit information (for internal use/backup).
-- Pattern: { Action = "Show-Credit", From = "<Owner>" }
Handlers.add(
  "Mgr-Query-All-Credits",
  Handlers.utils.hasMatchingTag("Action", "Show-Credit"),
  function (msg)
    -- Permission Check
    if not Permissions.is_owner(msg) then
      Logger.warn("Show-Credit denied: Sender " .. msg.From .. " is not the owner.")
      ao.send({ Target = msg.From, Tags = { Code = "403", Error = "Unauthorized" }})
      return
    end

    Logger.log("Owner requested all credit information.")
    local all_credits = PoolMgrDb:getAllCredits()
    ao.send({ Target = msg.From, Data = json.encode(all_credits) })
  end
)

--- Handler: Transfer-Credits
-- Description: Allows a user to transfer their unallocated credits to a specific pool.
-- Pattern: { Action = "Transfer-Credits" }
-- Message Data: { pool_id = "...", amount = "..." }
Handlers.add(
  "Mgr-Transfer-Credits",
  Handlers.utils.hasMatchingTag("Action", "Transfer-Credits"),
  function (msg)
    local user = msg.From
    local pool_id = msg.Data.pool_id
    local amount_to_transfer = msg.Data.amount

    -- Validate input
    if not pool_id or type(pool_id) ~= "string" or pool_id == "" or pool_id == "0" then
       Logger.error("Transfer-Credits failed: Invalid pool_id from " .. user)
       ao.send({ Target = user, Tags = { Code = "400", Error = "Invalid pool_id provided." }})
       return
    end
    if not amount_to_transfer or not BintUtils.is_valid(amount_to_transfer) or BintUtils.le(amount_to_transfer, '0') then
       Logger.error("Transfer-Credits failed: Invalid amount from " .. user)
       ao.send({ Target = user, Tags = { Code = "400", Error = "Invalid amount provided. Must be a positive integer string." }})
       return
    end

    -- Check if pool exists
    local pool_info = PoolMgrDb:getPool(pool_id)
    if not pool_info then
        Logger.error("Transfer-Credits failed: Pool " .. pool_id .. " not found.")
        ao.send({ Target = user, Tags = { Code = "404", Error = "Target pool not found." }})
        return
    end

    -- Check user's unallocated balance
    local unallocated_balance = PoolMgrDb:getCurrentCredits(user, '0')
    if BintUtils.lt(unallocated_balance, amount_to_transfer) then
        Logger.warn("Transfer-Credits failed: Insufficient unallocated credits for user " .. user .. ". Balance: " .. unallocated_balance .. ", Requested: " .. amount_to_transfer)
        ao.send({ Target = user, Tags = { Code = "403", Error = "Insufficient unallocated credits." }})
        return
    end

    Logger.log("Processing credit transfer for " .. user .. " to pool " .. pool_id .. ", Amount: " .. amount_to_transfer)

    -- Perform the transfer atomically (ideally within a DB transaction)
    -- 1. Deduct from unallocated ('0')
    local success_deduct = PoolMgrDb:recordCreditTransaction(user, 'transfer_out', BintUtils.mul(amount_to_transfer, '-1'), '0')
    -- 2. Add to target pool
    local success_add = false
    if success_deduct then
        success_add = PoolMgrDb:recordCreditTransaction(user, 'transfer_in', amount_to_transfer, pool_id)
    end

    if success_deduct and success_add then
        Logger.log("Credit transfer successful for " .. user .. " to pool " .. pool_id)
        -- Send notification to the target Pool process
        ao.send({
            Target = pool_id,
            Action = "AN-Credit-Notice", -- As defined in Pool LLD
            From = ao.id, -- Send from this Pool Mgr process
            User = user,
            Quantity = amount_to_transfer
        })
        -- Send confirmation back to user
        local new_unallocated = PoolMgrDb:getCurrentCredits(user, '0')
        local new_pool_balance = PoolMgrDb:getCurrentCredits(user, pool_id)
        ao.send({ Target = user, Tags = { Code = "200", Action = "Credits-Transferred" }, Data = json.encode({ pool_id = pool_id, amount = amount_to_transfer, new_unallocated_balance = new_unallocated, new_pool_balance = new_pool_balance }) })
    else
        Logger.error("Credit transfer failed for " .. user .. " to pool " .. pool_id .. ". Rolling back.")
        -- Rollback logic (needs DB transaction support ideally)
        if success_deduct and not success_add then
            -- Refund the deduction from unallocated
            PoolMgrDb:recordCreditTransaction(user, 'transfer_refund', amount_to_transfer, '0')
        end
        -- If deduction failed initially, no rollback needed for addition.
        ao.send({ Target = user, Tags = { Code = "500", Error = "Credit transfer failed internally." }})
    end
  end
)


-- ================= Staking Handlers =================

--- Handler: Stake (via APUS Transfer)
-- Description: Handles incoming APUS transfers intended for staking in a specific pool.
-- Pattern: { Action = "Credit-Notice", From = "<APUS Token ID>", ['X-AN-Reason'] = "Stake", ['X-AN-Pool-Id'] = "<pool_id>" }
Handlers.add(
  "Mgr-Stake",
  function(msg)
    return msg.Action == "Credit-Notice" and
           msg.From == ApusTokenId and
           msg.Tags['X-AN-Reason'] == 'Stake' and
           msg.Tags['X-AN-Pool-Id'] and -- Pool ID must be specified
           msg.Sender
  end,
  function (msg)
    local user = msg.Sender
    local apus_amount = msg.Quantity
    local pool_id = msg.Tags['X-AN-Pool-Id']

    if not user or not apus_amount or not BintUtils.is_valid(apus_amount) or BintUtils.le(apus_amount, '0') then
      Logger.error("Invalid Stake notice: Missing Sender, Quantity, or invalid Quantity. Msg: " .. json.encode(msg))
      return -- Cannot refund easily
    end
    if not pool_id then
       Logger.error("Invalid Stake notice: Missing X-AN-Pool-Id tag. Msg: " .. json.encode(msg))
       sendApus(user, apus_amount, "Refund: Stake Pool ID Missing")
       return
    end

    Logger.log("Processing Stake from User: " .. user .. ", APUS Quantity: " .. apus_amount .. ", Pool: " .. pool_id)

    -- Check if pool exists and get capacity
    local pool_info = PoolMgrDb:getPool(pool_id)
    if not pool_info then
        Logger.error("Stake failed: Pool " .. pool_id .. " not found.")
        sendApus(user, apus_amount, "Refund: Stake Pool Not Found")
        ao.send({ Target = user, Tags = { Code = "404", Error = "Target pool for staking not found.", Action="Stake-Failure" }})
        return
    end

    -- Check staking capacity
    local current_pool_stake = PoolMgrDb:getTotalPoolStake(pool_id)
    local capacity = pool_info.staking_capacity
    local potential_new_total = BintUtils.add(current_pool_stake, apus_amount)

    if BintUtils.gt(potential_new_total, capacity) then
        Logger.warn("Stake failed: Staking amount " .. apus_amount .. " exceeds pool " .. pool_id .. " capacity (" .. capacity .. "). Current stake: " .. current_pool_stake)
        sendApus(user, apus_amount, "Refund: Stake Exceeds Pool Capacity")
        ao.send({ Target = user, Tags = { Code = "403", Error = "Staking amount exceeds pool capacity.", Action="Stake-Failure" }})
        return
    end

    -- Record staking transaction
    local success = PoolMgrDb:recordStakingTransaction(user, pool_id, 'STAKE', apus_amount)

    if success then
      local new_stake_balance = PoolMgrDb:getCurrentStake(user, pool_id)
      Logger.log("Stake successful for " .. user .. " in pool " .. pool_id .. ". New stake balance: " .. new_stake_balance)
      ao.send({ Target = user, Tags = { Code = "200", Action = "Stake-Success" }, Data = json.encode({ pool_id = pool_id, staked_amount = apus_amount, new_stake_balance = new_stake_balance }) })
    else
      Logger.error("Failed to record staking transaction for " .. user .. " in pool " .. pool_id)
      sendApus(user, apus_amount, "Refund: Stake DB Error")
      ao.send({ Target = user, Tags = { Code = "500", Error = "Failed to process stake, APUS refunded.", Action="Stake-Failure" }})
    end
  end
)

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
       ao.send({ Target = user, Tags = { Code = "400", Error = "Invalid pool_id provided.", Action="UnStake-Failure" }})
       return
    end
    if not amount_to_unstake or not BintUtils.is_valid(amount_to_unstake) or BintUtils.le(amount_to_unstake, '0') then
       Logger.error("UnStake failed: Invalid amount from " .. user)
       ao.send({ Target = user, Tags = { Code = "400", Error = "Invalid amount provided. Must be a positive integer string.", Action="UnStake-Failure" }})
       return
    end

     -- Check if pool exists (sanity check)
    local pool_info = PoolMgrDb:getPool(pool_id)
    if not pool_info then
        Logger.error("UnStake failed: Pool " .. pool_id .. " not found.")
        ao.send({ Target = user, Tags = { Code = "404", Error = "Target pool for unstaking not found.", Action="UnStake-Failure" }})
        return
    end

    -- Check user's staked balance in that pool
    local current_stake = PoolMgrDb:getCurrentStake(user, pool_id)
    if BintUtils.lt(current_stake, amount_to_unstake) then
        Logger.warn("UnStake failed: Insufficient staked balance for user " .. user .. " in pool " .. pool_id .. ". Staked: " .. current_stake .. ", Requested: " .. amount_to_unstake)
        ao.send({ Target = user, Tags = { Code = "403", Error = "Insufficient staked balance.", Action="UnStake-Failure" }})
        return
    end

    Logger.log("Processing UnStake for " .. user .. " from pool " .. pool_id .. ", Amount: " .. amount_to_unstake)

    -- Record unstaking transaction (updates current_stakes)
    local success = PoolMgrDb:recordStakingTransaction(user, pool_id, 'UNSTAKE', amount_to_unstake)

    if success then
        -- Send APUS back to the user
        sendApus(user, amount_to_unstake, "Unstake from Pool " .. pool_id)

        local new_stake_balance = PoolMgrDb:getCurrentStake(user, pool_id)
        Logger.log("UnStake successful for " .. user .. " from pool " .. pool_id .. ". APUS sent. New stake balance: " .. new_stake_balance)
        ao.send({ Target = user, Tags = { Code = "200", Action = "UnStake-Success" }, Data = json.encode({ pool_id = pool_id, unstaked_amount = amount_to_unstake, new_stake_balance = new_stake_balance }) })
    else
        Logger.error("Failed to record unstaking transaction for " .. user .. " from pool " .. pool_id)
        ao.send({ Target = user, Tags = { Code = "500", Error = "Failed to process unstake.", Action="UnStake-Failure" }})
    end
  end
)

--- Handler: Get-Staking
-- Description: Gets the user's current staking balance(s).
-- Pattern: { Action = "Query-Staking" }
-- Optional Tag: Pool-Id = "<pool_id>"
Handlers.add(
  "Mgr-Get-Staking",
  Handlers.utils.hasMatchingTag("Action", "Query-Staking"),
  function (msg)
      local user = msg.From
      local pool_id_filter = msg.Tags['Pool-Id']

      if pool_id_filter then
          -- Query for a specific pool
          Logger.log("User " .. user .. " requested staking balance for pool " .. pool_id_filter)
          local balance = PoolMgrDb:getCurrentStake(user, pool_id_filter)
          ao.send({ Target = user, Data = json.encode({ [pool_id_filter] = balance }) })
      else
          -- Query for all pools the user has staked in
          Logger.log("User " .. user .. " requested all their staking balances.")
          local sql = [[ SELECT pool_id, amount FROM current_stakes WHERE user_wallet_address = ? AND amount != '0'; ]]
          local rows = PoolMgrDb.dbAdmin:select(sql, { user }) -- Access dbAdmin directly for custom query
          local balances = {}
          if rows then
              for _, row in ipairs(rows) do
                  balances[row.pool_id] = row.amount
              end
          end
          ao.send({ Target = user, Data = json.encode(balances) })
      end
  end
)

--- Handler: Get-Pool-Staking
-- Description: Gets the total staked amount in a specific pool.
-- Pattern: { Action = "Query-Pool-Staking" }
-- Required Tag: Pool-Id = "<pool_id>"
Handlers.add(
  "Mgr-Get-Pool-Staking",
  Handlers.utils.hasMatchingTag("Action", "Query-Pool-Staking"),
  function (msg)
      local pool_id = msg.Tags['Pool-Id']
      if not pool_id then
          Logger.warn("Query-Pool-Staking rejected: Missing Pool-Id tag from " .. msg.From)
          ao.send({ Target = msg.From, Tags = { Code = "400", Error = "Missing required Pool-Id tag." }})
          return
      end
      Logger.log("Request for total staking in pool " .. pool_id .. " from " .. msg.From)
      local total_stake = PoolMgrDb:getTotalPoolStake(pool_id)
      ao.send({ Target = msg.From, Data = json.encode({ pool_id = pool_id, total_staked = total_stake }) })
  end
)

--- Handler: Get-All-Staking (Owner Only)
-- Description: Returns all current staking information.
-- Pattern: { Action = "Get-All-Staking", From = "<Owner>" }
Handlers.add(
  "Mgr-Get-All-Staking",
  Handlers.utils.hasMatchingTag("Action", "Get-All-Staking"),
  function (msg)
    -- Permission Check
    if not Permissions.is_owner(msg) then
      Logger.warn("Get-All-Staking denied: Sender " .. msg.From .. " is not the owner.")
      ao.send({ Target = msg.From, Tags = { Code = "403", Error = "Unauthorized" }})
      return
    end

    Logger.log("Owner requested all staking information.")
    -- Choose which data to return: current stakes or full transaction history?
    -- LLD implies current records.
    local all_stakes = PoolMgrDb:getAllCurrentStakes()
    -- local all_transactions = PoolMgrDb:getAllStakingTransactions() -- Alternative
    ao.send({ Target = msg.From, Data = json.encode(all_stakes) })
  end
)

-- ================= Incentive Handlers =================

--- Handler: Distribute Interest (Cron)
-- Description: Calculates and distributes daily staking interest for each pool based on eligible stakes.
-- Pattern: { Action = "Cron" } -- Assuming cron messages have this action
Handlers.add(
  "Mgr-Distribute-Interest",
   Handlers.utils.hasMatchingTag("Action", "Cron"), -- Or specific tag if cron uses something else
  function (msg)
    -- Basic check to prevent accidental triggers if needed
    -- if msg.From ~= ao.id and msg.From ~= 'SOME_CRON_SCHEDULER_ID' then return end
    Logger.log("Cron job triggered: Distributing daily interest.")
    local current_time = math.floor(os.time()) -- Use ao.env.Timestamp?

    local all_pools = PoolMgrDb:getAllPools()
    if not all_pools or #all_pools == 0 then
        Logger.log("Distribute-Interest: No pools found to distribute interest for.")
        return
    end

    for _, pool in ipairs(all_pools) do
        local pool_id = pool.pool_id
        local daily_reward_R = pool.rewards_amount -- 'R' per pool

        if BintUtils.le(daily_reward_R, '0') then
            Logger.log("Distribute-Interest: Pool " .. pool_id .. " has no daily rewards (R=0). Skipping.")
            goto continue_pool -- Lua 5.1 doesn't have continue, use goto
        end

        local eligible_stakes = PoolMgrDb:getEligibleStakesForPool(pool_id, current_time)
        if #eligible_stakes == 0 then
            Logger.log("Distribute-Interest: No eligible stakes found for pool " .. pool_id)
            goto continue_pool
        end

        -- Calculate total eligible stake (S) for this pool
        local total_eligible_stake_S = '0'
        for _, stake in ipairs(eligible_stakes) do
            total_eligible_stake_S = BintUtils.add(total_eligible_stake_S, stake.amount)
        end

        if BintUtils.le(total_eligible_stake_S, '0') then
            Logger.log("Distribute-Interest: Total eligible stake is zero for pool " .. pool_id .. ". Skipping.")
            goto continue_pool
        end

        -- Calculate daily rate = R / S
        -- Note: BintUtils needs a 'div' function that handles potential decimals or precision.
        -- Assuming BintUtils.div returns a string representation of the rate, possibly with fixed precision.
        -- For simplicity, let's assume integer division for now, which is likely wrong for rates.
        -- A proper implementation needs careful handling of division and precision.
        -- Placeholder: dailyRate = daily_reward_R / total_eligible_stake_S
        -- Let's represent rate as a fraction string "R/S" or calculate with higher precision if BintUtils supports it.
        -- Using placeholder calculation - THIS NEEDS A REAL BINT DIVISION
        local daily_rate_str = "PLACEHOLDER_RATE" -- e.g., BintUtils.div(daily_reward_R, total_eligible_stake_S, 18) -- 18 decimal places

        Logger.log("Distribute-Interest for Pool " .. pool_id .. ": R=" .. daily_reward_R .. ", S=" .. total_eligible_stake_S .. ", Rate=" .. daily_rate_str)

        for _, stake in ipairs(eligible_stakes) do
            local user = stake.user_wallet_address
            local user_stake = stake.amount

            -- Calculate interest = userStake * dailyRate
            -- Again, needs proper Bint multiplication with precision.
            -- Placeholder: interest = user_stake * daily_rate
            -- Assuming BintUtils.mul handles this and returns integer part. NEEDS REAL BINT MULTIPLICATION
            local interest_amount = "PLACEHOLDER_INTEREST" -- e.g., BintUtils.mul(user_stake, daily_rate_str, 18) -- Multiply then maybe truncate/round

            if BintUtils.gt(interest_amount, '0') then
                -- Record distribution
                PoolMgrDb:recordInterestDistribution(user, pool_id, interest_amount, user_stake, daily_rate_str)
                -- Send APUS interest to user
                sendApus(user, interest_amount, "Daily Staking Interest from Pool " .. pool_id)
            else
                 Logger.log("Calculated interest is zero for user " .. user .. " in pool " .. pool_id)
            end
        end
        ::continue_pool:: -- Goto label for skipping pool iteration
    end
    Logger.log("Daily interest distribution cycle finished.")
  end
)

-- ================= Pool Management Handlers =================

--- Handler: Create-Pool (Added based on HLD)
-- Description: Creates a new Pool process and registers it (Owner only for now).
-- Pattern: { Action = "Create-Pool", From = "<Owner>" }
-- Message Data: { staking_capacity = "...", rewards_amount = "..." } (Optional, uses defaults otherwise)
Handlers.add(
  "Mgr-Create-Pool",
  Handlers.utils.hasMatchingTag("Action", "Create-Pool"),
  function (msg)
     -- Permission Check
    if not Permissions.is_owner(msg) then
      Logger.warn("Create-Pool denied: Sender " .. msg.From .. " is not the owner.")
      ao.send({ Target = msg.From, Tags = { Code = "403", Error = "Unauthorized" }})
      return
    end

    local creator = msg.From -- Owner is the creator for now
    local capacity = msg.Data.staking_capacity or Config.DefaultPoolStakingCapacity
    local rewards = msg.Data.rewards_amount or Config.DefaultPoolRewardsAmount

    if not BintUtils.is_valid(capacity) or BintUtils.lt(capacity, '0') or
       not BintUtils.is_valid(rewards) or BintUtils.lt(rewards, '0') then
        Logger.error("Create-Pool failed: Invalid capacity or rewards amount.")
        ao.send({ Target = msg.From, Tags = { Code = "400", Error = "Invalid capacity or rewards amount." }})
        return
    end

    Logger.log("Owner requested pool creation. Capacity: " .. capacity .. ", Rewards: " .. rewards)

    -- 1. Spawn the new Pool process
    -- Need the source code of the Pool process (pool.lua) - How to get this?
    -- Assume `PoolSourceCodeTxId` is known (e.g., from deployment)
    local PoolSourceCodeTxId = "POOL_SOURCE_CODE_TX_ID_PLACEHOLDER"
    local spawn_msg = {
        Module = PoolSourceCodeTxId,
        Scheduler = ao.env.Process.Scheduler, -- Use same scheduler?
        Tags = {
            { name = "Name", value = "ANPM-Pool-" .. math.random(1000, 9999) }, -- Generate a name
            { name = "Creator", value = creator },
            { name = "PoolManager", value = ao.id } -- Pass Pool Mgr ID to Pool
            -- Add other necessary tags for the Pool process initialization
        },
        Data = json.encode({ -- Pass initial config via Data? Or tags?
             Owner = creator, -- Pool owner might be the creator or the Mgr owner? Let's use creator.
             PoolMgrProcessId = ao.id,
             -- TaskCost could be set here too
        })
    }
    local new_pool_id = ao.spawn(spawn_msg) -- Check ao-llms.md for exact spawn syntax/return

    if not new_pool_id or new_pool_id == "" then -- Check how spawn indicates failure
        Logger.error("Failed to spawn new Pool process.")
        ao.send({ Target = msg.From, Tags = { Code = "500", Error = "Failed to spawn Pool process." }})
        return
    end

    Logger.log("Successfully spawned new Pool process: " .. new_pool_id)

    -- 2. Register the new pool in the database
    local success_register = PoolMgrDb:addPool(new_pool_id, creator, capacity, rewards)

    if success_register then
        Logger.log("New Pool " .. new_pool_id .. " registered successfully.")
        ao.send({ Target = msg.From, Tags = { Code = "200", Action = "Pool-Created" }, Data = json.encode({ pool_id = new_pool_id }) })
    else
        Logger.error("Failed to register newly spawned Pool " .. new_pool_id .. " in database.")
        -- Pool process exists but isn't tracked by manager. Critical error.
        -- TODO: Should we try to kill the spawned process? Or just log?
        ao.send({ Target = msg.From, Tags = { Code = "500", Error = "Pool spawned but failed to register." }, Data = json.encode({ pool_id = new_pool_id }) })
    end
  end
)

--- Handler: Info
-- Description: Returns basic info about the Pool Manager.
-- Pattern: { Action = "Info" }
Handlers.add(
  "Mgr-Info",
  Handlers.utils.hasMatchingTag("Action", "Info"),
  function (msg)
      Logger.log("Request for Pool Manager info from " .. msg.From)
      local info = {
          process_id = ao.id,
          owner = Owner,
          apus_token = ApusTokenId,
          credit_exchange_rate = CreditExchangeRate,
          registered_pools_count = #PoolMgrDb:getAllPools(), -- Count pools
          -- Add other relevant info
      }
      ao.send({ Target = msg.From, Data = json.encode(info) })
  end
)


-- Error Handler (Generic)
Handlers.add(
  "Mgr-ErrorHandler",
  Handlers.utils.isError(),
  function (msg)
    Logger.error("PoolMgr Generic Error Handler caught: " .. msg.Error)
    -- Optional: Send error details back to sender if appropriate
  end
)

Logger.log("Pool Manager Process Handlers Loaded.")
