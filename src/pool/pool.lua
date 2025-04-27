-- Pool Process Implementation
-- Manages AI tasks, user credits within the pool, and GPU Node Oracles.

-- AO Library (Implicitly available)
-- local ao = require('ao') -- Assuming 'ao' is globally available or required implicitly

-- Utilities
local Logger = require('utils.log')
local BintUtils = require('utils.bint_utils')
local Permissions = require('utils.permissions')
local PoolDb = require('pool.pool_db').new() -- Initialize DAL
local Config = require('pool.config')

-- Process State (In-memory, persisted via AO mechanisms)
-- Credits: Map<wallet_address, bigint_string>
local Credits = Credits or {} -- Load from state if available
-- Oracles: Map<node_id, owner_address>
local Oracles = Oracles or {} -- Load from state if available

-- Constants from Config
local Owner = Config.Owner
local PoolMgrId = Config.PoolMgrProcessId
local TaskCost = Config.TaskCost

Logger.log('Pool Process (' .. ao.id .. ') Started. Owner: ' .. Owner .. ', PoolMgr: ' .. PoolMgrId)

-- ================= Handlers =================

--- Handler: Add-Credit
-- Description: Adds credit to a user's balance, only accepts messages from the configured Pool Manager.
-- Pattern: { Action = "AN-Credit-Notice", From = "<Pool Mgr Process ID>", Quantity = "_", User = "_" }
Handlers.add(
  "Pool-Add-Credit",
  Handlers.utils.hasMatchingTag("Action", "AN-Credit-Notice"),
  function (msg)
    Logger.log("Received AN-Credit-Notice from " .. msg.From)
    -- Permission Check: Must be from Pool Manager
    if msg.From ~= PoolMgrId then
      Logger.warn("Permission Denied: AN-Credit-Notice from non-PoolMgr address " .. msg.From)
      -- No response needed for unauthorized credit notice
      return
    end

    local user = msg.Tags.User
    local quantity = msg.Tags.Quantity

    if not user or not quantity or not BintUtils.is_valid(quantity) or BintUtils.lt(quantity, '0') then
      Logger.error("Invalid AN-Credit-Notice: Missing User, Quantity, or invalid Quantity format. Msg: " .. json.encode(msg))
      -- Maybe send error back to PoolMgr? For now, just log.
      return
    end

    Logger.log("Processing Add-Credit for User: " .. user .. ", Quantity: " .. quantity)

    -- Update Credits Map
    local current_balance = Credits[user] or '0'
    Credits[user] = BintUtils.add(current_balance, quantity)

    Logger.log("User " .. user .. " new credit balance: " .. Credits[user])

    -- Send confirmation back to Pool Manager? LLD doesn't specify.
    -- Send confirmation back to User? LLD doesn't specify.
    -- For now, just update state.
    print("Credit added successfully for user " .. user .. ". New balance: " .. Credits[user])
  end
)

--- Handler: Get-Credit-Balance
-- Description: Allows a user to query their credit balance within this pool.
-- Pattern: { Action = "Credit-Balance" }
Handlers.add(
  "Pool-Get-Credit-Balance",
  Handlers.utils.hasMatchingTag("Action", "Credit-Balance"),
  function (msg)
    local user = msg.From
    local balance = Credits[user] or '0'
    Logger.log("User " .. user .. " requested credit balance. Balance: " .. balance)
    ao.send({ Target = user, Data = balance })
  end
)

--- Handler: Add-Task
-- Description: Accepts a task from a user, checks/deducts credit, and adds it to the database.
-- Pattern: { Action = "Add-Task" }
-- Message Data: { prompt = "...", config = "..." } (Config is optional)
Handlers.add(
  "Pool-Add-Task",
  Handlers.utils.hasMatchingTag("Action", "Add-Task"),
  function (msg)
    local user = msg.From
    local prompt = msg.Data.prompt
    local config_str = msg.Data.config -- Optional

    if not prompt or type(prompt) ~= "string" or prompt == "" then
       Logger.error("Add-Task failed: Missing or invalid prompt from " .. user)
       ao.send({ Target = user, Tags = { Code = "400", Error = "Missing or invalid prompt" }})
       return
    end

    -- Check User Credit Balance
    local current_balance = Credits[user] or '0'
    if BintUtils.lt(current_balance, TaskCost) then
      Logger.warn("Add-Task failed: Insufficient credits for user " .. user .. ". Balance: " .. current_balance .. ", Cost: " .. TaskCost)
      ao.send({ Target = user, Tags = { Code = "403", Error = "Insufficient credits" }})
      return
    end

    -- Deduct Credit
    Credits[user] = BintUtils.sub(current_balance, TaskCost)
    Logger.log("Deducted " .. TaskCost .. " credits from user " .. user .. ". New balance: " .. Credits[user])

    -- Add Task to Database
    local task_ref = PoolDb:addTask(user, prompt, config_str)

    if task_ref then
      Logger.log("Task added successfully for user " .. user .. ". Ref: " .. task_ref)
      ao.send({ Target = user, Tags = { Code = "200", Action = "Task-Added", Ref = tostring(task_ref) }})
    else
      Logger.error("Failed to add task to database for user " .. user)
      -- Refund credit?
      Credits[user] = BintUtils.add(Credits[user], TaskCost) -- Refund
      Logger.log("Refunded " .. TaskCost .. " credits to user " .. user .. " due to DB error. Balance: " .. Credits[user])
      ao.send({ Target = user, Tags = { Code = "500", Error = "Failed to add task to database" }})
    end
  end
)

--- Handler: Get-Pending-Task
-- Description: Finds a pending task, marks it as processing, and returns it to a registered Oracle.
-- Pattern: { Action = "Get-Pending-Task" }
Handlers.add(
  "Pool-Get-Pending-Task",
  Handlers.utils.hasMatchingTag("Action", "Get-Pending-Task"),
  function (msg)
    local oracle_owner = msg.From
    local oracle_node_id = msg.Tags['X-Oracle-Node-Id'] -- Oracles should identify themselves with their Node ID

    -- Permission Check: Must be a registered Oracle owner AND provide their node ID
    local registered_node_id = nil
    for node_id, owner in pairs(Oracles) do
        if owner == oracle_owner then
            registered_node_id = node_id
            break
        end
    end

    if not registered_node_id then
       Logger.warn("Get-Pending-Task denied: Sender " .. oracle_owner .. " is not a registered Oracle owner.")
       ao.send({ Target = oracle_owner, Tags = { Code = "403", Error = "Unauthorized: Not a registered Oracle owner" }})
       return
    end

    -- If Oracle provided Node ID, verify it matches the registered one for that owner
    if oracle_node_id and oracle_node_id ~= registered_node_id then
        Logger.warn("Get-Pending-Task denied: Sender " .. oracle_owner .. " provided mismatched Node ID '" .. oracle_node_id .. "'. Expected '" .. registered_node_id .. "'")
        ao.send({ Target = oracle_owner, Tags = { Code = "403", Error = "Unauthorized: Mismatched Oracle Node ID" }})
        return
    end

    -- Use the registered node ID associated with the owner
    local node_id_to_assign = registered_node_id
    Logger.log("Oracle " .. oracle_owner .. " (Node ID: " .. node_id_to_assign .. ") requested a pending task.")

    -- Get and start the task
    local task = PoolDb:getAndStartPendingTask(node_id_to_assign)

    if task then
      Logger.log("Assigning task " .. task.ref .. " to Oracle " .. node_id_to_assign)
      -- Send task details to the Oracle
      ao.send({
        Target = oracle_owner,
        Tags = { Code = "200", Action = "Task-Assignment" },
        Data = json.encode({
          ref = task.ref,
          prompt = task.prompt,
          config = task.config
          -- Do not send submitter info to Oracle
        })
      })
    else
      Logger.log("No pending tasks available for Oracle " .. node_id_to_assign)
      ao.send({ Target = oracle_owner, Tags = { Code = "204", Action = "No-Pending-Tasks" }})
    end
  end
)

--- Handler: Task-Response
-- Description: Accepts a task result from an Oracle, updates the task status, and notifies the original submitter.
-- Pattern: { Action = "Task-Response" }
-- Message Tags: { 'X-Reference' = "<task_ref>", 'X-Oracle-Node-Id' = "<node_id>" }
-- Message Data: { output = "..." }
Handlers.add(
  "Pool-Task-Response",
  Handlers.utils.hasMatchingTag("Action", "Task-Response"),
  function (msg)
    local oracle_owner = msg.From
    local task_ref_str = msg.Tags['X-Reference']
    local oracle_node_id = msg.Tags['X-Oracle-Node-Id'] -- Oracle identifies itself
    local output = msg.Data.output

    if not task_ref_str or not tonumber(task_ref_str) then
        Logger.error("Task-Response failed: Missing or invalid X-Reference tag from " .. oracle_owner)
        ao.send({ Target = oracle_owner, Tags = { Code = "400", Error = "Missing or invalid X-Reference tag" }})
        return
    end
    local task_ref = tonumber(task_ref_str)

    if not oracle_node_id then
        Logger.error("Task-Response failed: Missing X-Oracle-Node-Id tag from " .. oracle_owner)
        ao.send({ Target = oracle_owner, Tags = { Code = "400", Error = "Missing X-Oracle-Node-Id tag" }})
        return
    end

    if not output or type(output) ~= "string" then
        Logger.error("Task-Response failed: Missing or invalid output data from " .. oracle_owner .. " for task " .. task_ref)
        ao.send({ Target = oracle_owner, Tags = { Code = "400", Error = "Missing or invalid output data" }})
        return
    end

    -- Permission/Validation Check: Is the sender the owner of the Oracle Node ID provided?
    if Oracles[oracle_node_id] ~= oracle_owner then
        Logger.warn("Task-Response denied: Sender " .. oracle_owner .. " is not the registered owner of Oracle Node ID " .. oracle_node_id)
        ao.send({ Target = oracle_owner, Tags = { Code = "403", Error = "Unauthorized: Sender does not own the specified Oracle Node ID" }})
        return
    end

    Logger.log("Received Task-Response for ref " .. task_ref .. " from Oracle " .. oracle_node_id .. " (Owner: " .. oracle_owner .. ")")

    -- Update task in DB, verifying the oracle node ID matches the one assigned
    local updated_task, err = PoolDb:setTaskResponse(task_ref, output, oracle_node_id)

    if updated_task then
      Logger.log("Task " .. task_ref .. " successfully updated to done.")
      -- Send success confirmation to Oracle
      ao.send({ Target = oracle_owner, Tags = { Code = "200", Action = "Response-Accepted", Ref = tostring(task_ref) }})

      -- Notify the original submitter
      local submitter = updated_task.submitter
      if submitter then
         Logger.log("Notifying submitter " .. submitter .. " about completed task " .. task_ref)
         ao.send({
             Target = submitter,
             Tags = { Action = "Task-Result", Code = "200", Ref = tostring(task_ref) },
             Data = updated_task.output -- Send only the output
         })
      else
         Logger.warn("Could not notify submitter for task " .. task_ref .. ": Submitter address not found in updated task data.")
      end

    else
      Logger.error("Failed to set task response for ref " .. task_ref .. ". Error: " .. (err or "Unknown DB error"))
      -- Send error back to Oracle
      ao.send({ Target = oracle_owner, Tags = { Code = "404", -- Or 403 if Oracle mismatch, 500 if DB error
                                                Action = "Response-Rejected",
                                                Ref = tostring(task_ref),
                                                Error = err or "Failed to update task status" }})
    end
  end
)

--- Handler: Tasks-Statistics
-- Description: Returns statistics about tasks in the pool.
-- Pattern: { Action = "Tasks-Statistics" }
Handlers.add(
  "Pool-Tasks-Statistics",
  Handlers.utils.hasMatchingTag("Action", "Tasks-Statistics"),
  function (msg)
    Logger.log("Request for task statistics from " .. msg.From)
    local stats = PoolDb:getTaskStatistics()
    ao.send({ Target = msg.From, Data = json.encode(stats) })
  end
)

--- Handler: Add-Node-Oracle
-- Description: Allows the process owner to add a new GPU Node Oracle.
-- Pattern: { Action = "Add-Node-Oracle", From = "<Process Owner>" }
-- Message Data: { node_id = "...", owner = "..." }
Handlers.add(
  "Pool-Add-Node-Oracle",
  Handlers.utils.hasMatchingTag("Action", "Add-Node-Oracle"),
  function (msg)
    -- Permission Check: Must be Owner
    -- NOTE: Permissions.is_owner requires 'Owner' to be in scope.
    if not Permissions.is_owner(msg) then
      Logger.warn("Add-Node-Oracle denied: Sender " .. msg.From .. " is not the owner.")
      ao.send({ Target = msg.From, Tags = { Code = "403", Error = "Unauthorized" }})
      return
    end

    local node_id = msg.Data.node_id
    local oracle_owner = msg.Data.owner

    if not node_id or type(node_id) ~= "string" or node_id == "" or
       not oracle_owner or type(oracle_owner) ~= "string" or oracle_owner == "" then
       Logger.error("Add-Node-Oracle failed: Missing or invalid node_id or owner in Data.")
       ao.send({ Target = msg.From, Tags = { Code = "400", Error = "Missing or invalid node_id or owner in Data" }})
       return
    end

    if Oracles[node_id] then
       Logger.warn("Add-Node-Oracle failed: Oracle Node ID " .. node_id .. " already exists.")
       ao.send({ Target = msg.From, Tags = { Code = "409", Error = "Oracle Node ID already exists" }})
       return
    end

    Oracles[node_id] = oracle_owner
    Logger.log("Oracle Node added/updated: ID=" .. node_id .. ", Owner=" .. oracle_owner)
    ao.send({ Target = msg.From, Tags = { Code = "200", Action = "Oracle-Added" }})
  end
)

--- Handler: Remove-Node-Oracle
-- Description: Allows the process owner to remove a GPU Node Oracle.
-- Pattern: { Action = "Remove-Node-Oracle", From = "<Process Owner>" }
-- Message Data: { node_id = "..." }
Handlers.add(
  "Pool-Remove-Node-Oracle",
  Handlers.utils.hasMatchingTag("Action", "Remove-Node-Oracle"),
  function (msg)
    -- Permission Check: Must be Owner (Corrected from LLD)
    if not Permissions.is_owner(msg) then
      Logger.warn("Remove-Node-Oracle denied: Sender " .. msg.From .. " is not the owner.")
      ao.send({ Target = msg.From, Tags = { Code = "403", Error = "Unauthorized" }})
      return
    end

    local node_id = msg.Data.node_id

    if not node_id or type(node_id) ~= "string" or node_id == "" then
       Logger.error("Remove-Node-Oracle failed: Missing or invalid node_id in Data.")
       ao.send({ Target = msg.From, Tags = { Code = "400", Error = "Missing or invalid node_id in Data" }})
       return
    end

    if not Oracles[node_id] then
      Logger.warn("Remove-Node-Oracle failed: Oracle Node ID " .. node_id .. " not found.")
      ao.send({ Target = msg.From, Tags = { Code = "404", Error = "Oracle Node ID not found" }})
      return
    end

    Oracles[node_id] = nil
    Logger.log("Oracle Node removed: ID=" .. node_id)
    ao.send({ Target = msg.From, Tags = { Code = "200", Action = "Oracle-Removed" }})
  end
)

--- Handler: Get-Node-Oracles
-- Description: Allows anyone to get the list of registered Oracle nodes and their owners.
-- Pattern: { Action = "Get-Node-Oracles" }
Handlers.add(
  "Pool-Get-Node-Oracles",
  Handlers.utils.hasMatchingTag("Action", "Get-Node-Oracles"),
  function (msg)
    Logger.log("Request for node oracles list from " .. msg.From)
    local oracle_list = {}
    for node_id, owner in pairs(Oracles) do
        table.insert(oracle_list, { node_id = node_id, owner = owner }) -- LLD uses 'uuid', using 'node_id' consistently
    end
    ao.send({ Target = msg.From, Data = json.encode(oracle_list) })
  end
)

-- Error Handler (Generic)
Handlers.add(
  "ErrorHandler",
  Handlers.utils.isError(),
  function (msg)
    Logger.error("Generic Error Handler caught: " .. msg.Error)
    -- Optional: Send error details back to sender if appropriate
    -- ao.send({ Target = msg.From, Error = "An internal error occurred: " .. msg.Error })
  end
)

Logger.log("Pool Process Handlers Loaded.")
