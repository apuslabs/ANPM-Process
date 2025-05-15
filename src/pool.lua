-- Pool Process Implementation
-- Manages AI tasks, user credits within the pool, and GPU Node Oracles.

-- AO Library (Implicitly available)
-- local ao = require('ao') -- Assuming 'ao' is globally available or required implicitly

-- Utilities
local JSON = require("json")
local Logger = require('utils.log')
local BintUtils = require('utils.bint_utils')
local PoolDb = require('dao.pool_db').new() -- Initialize DAL
local Config = require('utils.config')
Logger.LogLevel = "trace"

-- Process State (In-memory, persisted via AO mechanisms)
-- Credits: Map<wallet_address, bigint_string>
Credits = Credits or {} -- Load from state if available
-- Oracles: Map<node_id, owner_address>
Oracles = Oracles or {} -- Load from state if available

-- Constants from Config
POOL_MGR_PROCESS_ID = POOL_MGR_PROCESS_ID or Config.PoolMgrProcessId
TASK_COST = Config.TaskCost

-- ================= Handlers =================

--- Handler: Add-Credit
-- Description: Adds credit to a user's balance, only accepts messages from the configured Pool Manager.
Handlers.add(
  "Add-Credit",
  { Action = "AN-Credit-Notice"},
  function(msg)
    local user = msg.Tags.User
    local quantity = msg.Tags.Quantity
    local from  = msg.From
    assert(from == POOL_MGR_PROCESS_ID, "Only accept Add-Credit from Pool Manager")
    assert(type(user) == 'string', "Add-Credit requires a user")
    Logger.trace("Processing Add-Credit for User: " .. user .. ", Quantity: " .. quantity)

    -- Update Credits Map
    local current_balance = Credits[user] or '0'
    Credits[user] = BintUtils.add(current_balance, quantity)

    Logger.trace("User " .. user .. " new credit balance: " .. Credits[user])

    -- Send confirmation back to Pool Manager? LLD doesn't specify.
    -- Send confirmation back to User? LLD doesn't specify.
    -- For now, just update state.
    ao.send(
      {
        Target = user,
        Tags = { Code = "200", Action = "Credit-Added" },
        Data = JSON.encode({ user = user, balance = Credits[user] })
      }
    )
  end
)

--- Handler: Transfer-Credits
-- Description: Transfer credits back to Pool Manager.
Handlers.add(
  "Transfer-Credits",
  { Action = "Transfer-Credits"},
  function(msg)
    local user = msg.From
    local quantity = msg.Tags.Quantity
    assert(type(user) == 'string', "Transfer-Creditst requires a user")
    Logger.trace("Processing Transfer-Credits for User: " .. user .. ", Quantity: " .. quantity)

    -- Check if user has enough credits
    local current_balance = Credits[user] or '0'
    if BintUtils.lt(current_balance, quantity) then
      Logger.warn("Transfer-Credits failed: Insufficient credits for user " ..
        user .. ". Balance: " .. current_balance .. ", Transfer Amount: " .. quantity)
      msg.reply({
        Tags = { Code = "403" },
        Data = "Insufficient credits"
      })
      return
    end

    -- Deduct Credit
    Credits[user] = BintUtils.subtract(current_balance, quantity)

    -- Send confirmation back to Pool Manager
    ao.send(
      {
        Target = POOL_MGR_PROCESS_ID,
        Tags = { Action = "AN-Credit-Notice", User = user, Quantity = quantity },
      }
    )
  end
)

--- Handler: Get-Credit-Balance
-- Description: Allows a user to query their credit balance within this pool.
Handlers.add(
  "Get-Credit-Balance",
  { Action = "Credit-Balance" },
  function(msg)
    local user = msg.Tags.Recipient or msg.From
    msg.reply({
      Tags = { Code = "200" },
      Data = JSON.encode({ user = user, balance = BintUtils.toBalanceValue(Credits[user] or '0') })
    })
  end
)

--- Handler: Add-Task
-- Description: Accepts a task from a user, checks/deducts credit, and adds it to the database.
Handlers.add(
  "Add-Task",
  { Action = "Add-Task" },
  function(msg)
    local user = msg.From
    local ref = msg.Tags["X-Reference"] or msg.Tags.Reference
    if not ref then
      Logger.error("Add-Task failed: Missing reference ID from " .. user)
      msg.reply({
        Tags = { Code = "400" },
        Data = "Missing reference ID"
      })
      return
    end
    local data = JSON.decode(msg.Data)
    local prompt = data.prompt
    local config = data.config or [[{"n_gpu_layers":48,"ctx_size":20480}]] -- Optional

    if not prompt or type(prompt) ~= "string" or prompt == "" then
      Logger.warn("Add-Task failed: Missing or invalid prompt from " .. user)
      msg.reply({
        Tags = { Code = "400" },
        Data = "Missing or invalid prompt"
      })
      return
    end

    -- Check User Credit Balance
    local current_balance = Credits[user] or '0'
    if BintUtils.lt(current_balance, TASK_COST) then
      Logger.warn("Add-Task failed: Insufficient credits for user " ..
        user .. ". Balance: " .. current_balance .. ", Cost: " .. TASK_COST)
      msg.reply({ Tags = { Code = "403" }, Data = "Insufficient credits" })
      return
    end

    Logger.trace("Processing Add-Task from User: " .. user .. ", Prompt: " .. prompt .. ", Config: " .. config)

    -- Deduct Credit
    Credits[user] = BintUtils.subtract(current_balance, TASK_COST)

    -- Add Task to Database
    PoolDb:addTask(ref, user, prompt, config)

    Logger.trace("Task added to database with ref: " .. ref .. ", User: " .. user .. ", Cost: " .. TASK_COST)
  end
)


-- Handler: Has-Pending-Task
-- Description: Checks if there are any pending tasks in the pool. for dryrun
Handlers.add(
  "Has-Pending-Task",
  { Action = "Has-Pending-Task" },
  function(msg)
    local has_pending_tasks = PoolDb:hasPendingTask()
    if has_pending_tasks then
      msg.reply({
        Tags = { Code = "200" },
      })
    else
      msg.reply({
        Tags = { Code = "204" },
      })
    end
  end
)

--- Handler: Get-Pending-Task
-- Description: Finds a pending task, marks it as processing, and returns it to a registered Oracle.
Handlers.add(
  "Get-Pending-Task",
  { Action = "Get-Pending-Task", NodeID = "_" },
  function(msg)
    local oracle_owner = msg.From
    local oracle_node_id = msg.Tags.NodeID -- Oracles should identify themselves with their Node ID

    -- Permission Check: Must be a registered Oracle owner AND provide their node ID
    local is_node_registered = false
    for node_id, owner in pairs(Oracles) do
      if owner == oracle_owner and node_id == oracle_node_id then
        is_node_registered = true
        break
      end
    end
    if not is_node_registered then
      Logger.warn("Get-Pending-Task denied: Sender " .. oracle_owner .. " is not a registered Oracle owner.")
      msg.reply({
        Tags = { Code = "403" }, Data = "Unauthorized: Not a registered Oracle"
      })
      return
    end

    -- Use the registered node ID associated with the owner

    -- Get and start the task
    local task = PoolDb:getAndStartPendingTask(oracle_node_id)

    if task then
      Logger.trace("Oracle " ..
      oracle_owner .. " (Node ID: " .. oracle_node_id .. ") requested a pending task " .. task.ref)
      -- Send task details to the Oracle
      msg.reply({
        Tags = { Code = "200" },
        Data = JSON.encode({
          ref = task.ref,
          prompt = task.prompt,
          config = task.config
        })
      })
    else
      Logger.trace("No pending tasks available for Oracle " .. oracle_node_id)
      msg.reply({
        Tags = { Code = "204" },
        Data = "No pending tasks available"
      })
    end
  end
)

--- Handler: Task-Response
-- Description: Accepts a task result from an Oracle, updates the task status, and notifies the original submitter.
Handlers.add(
  "Task-Response",
  { Action = "Task-Response", ["X-Oracle-Node-Id"] = "_", ['X-Reference'] = "_" },
  function(msg)
    local oracle_owner = msg.From
    local task_ref = msg.Tags['X-Reference']
    local oracle_node_id = msg.Tags['X-Oracle-Node-Id'] -- Oracle identifies itself
    local data = JSON.decode(msg.Data)
    local output = data.output

    if not output or type(output) ~= "string" then
      Logger.error("Task-Response failed: Missing or invalid output data from " ..
        oracle_owner .. " for task " .. task_ref)
      msg.reply({
        Tags = { Code = "400" },
        Data = "Missing or invalid output data"
      })
      return
    end

    -- Permission/Validation Check: Is the sender the owner of the Oracle Node ID provided?
    if Oracles[oracle_node_id] ~= oracle_owner then
      Logger.warn("Task-Response denied: Sender " ..
        oracle_owner .. " is not the registered owner of Oracle Node ID " .. oracle_node_id)
      msg.reply({
        Tags = { Code = "403" },
        Data = "Unauthorized: Sender does not own the specified Oracle Node ID"
      })
      return
    end

    Logger.trace("Received Task-Response for ref " ..
      task_ref .. " from Oracle " .. oracle_node_id .. " (Owner: " .. oracle_owner .. ")")

    -- Update task in DB, verifying the oracle node ID matches the one assigned
    local updated_task, err = PoolDb:setTaskResponse(task_ref, output, oracle_node_id)

    if updated_task then
      msg.forward(updated_task.submitter, {})
    else
      Logger.error("Failed to set task response for ref " .. task_ref .. ". Error: " .. (err or "Unknown DB error"))
    end
  end
)

--- Handler: Get-Task-Response
--- Description: Allows a user to query the status of their task.
Handlers.add(
  "Get-Task-Response",
  { Action = "Get-Task-Response" },
  function(msg)
    local task_ref = msg.Data
    if not task_ref then
      msg.reply({
        Tags = { Code = "400" },
        Data = "Missing or invalid task reference"
      })
      return
    end

    local task = PoolDb:getTaskByRef(task_ref)
    if task then
      msg.reply({
        Tags = { Code = "200" },
        Data = JSON.encode(task)
      })
    else
      msg.reply({
        Tags = { Code = "404" },
        Data = "No response available for the specified task reference"
      })
    end
  end
)

--- Handler: Tasks-Statistics
-- Description: Returns statistics about tasks in the pool.
Handlers.add(
  "Tasks-Statistics",
  { Action = "Tasks-Statistics" },
  function(msg)
    local stats = PoolDb:getTaskStatistics()
    msg.reply({ Data = JSON.encode(stats) })
  end
)

--- Handler: Add-Node-Oracle
-- Description: Allows the process owner to add a new GPU Node Oracle.
Handlers.add(
  "Add-Node-Oracle",
  { Action = "Add-Node-Oracle", From = function(from) return from == Owner or from == ao.id end },
  function(msg)
    local data = JSON.decode(msg.Data)
    local node_id = data.node_id
    local oracle_owner = data.owner

    if not node_id or type(node_id) ~= "string" or node_id == "" or
        not oracle_owner or type(oracle_owner) ~= "string" or oracle_owner == "" then
      Logger.error("Add-Node-Oracle failed: Missing or invalid node_id or owner in Data.")
      return
    end

    Oracles[node_id] = oracle_owner
    Logger.info("Oracle Node added/updated: ID=" .. node_id .. ", Owner=" .. oracle_owner)
  end
)

--- Handler: Remove-Node-Oracle
-- Description: Allows the process owner to remove a GPU Node Oracle.
Handlers.add(
  "Remove-Node-Oracle",
  { Action = "Remove-Node-Oracle", From = function(from) return from == Owner or from == ao.id end },
  function(msg)
    local data = JSON.decode(msg.Data)
    local node_id = data.node_id

    if not node_id or type(node_id) ~= "string" or node_id == "" then
      Logger.error("Remove-Node-Oracle failed: Missing or invalid node_id in Data.")
      return
    end

    Oracles[node_id] = nil
    Logger.info("Oracle Node removed: ID=" .. node_id)
  end
)

--- Handler: Get-Node-Oracles
-- Description: Allows anyone to get the list of registered Oracle nodes and their owners.
Handlers.add(
  "Get-Node-Oracles",
  { Action = "Get-Node-Oracles" },
  function(msg)
    local oracle_list = {}
    for node_id, owner in pairs(Oracles) do
      table.insert(oracle_list, { node_id = node_id, owner = owner })
    end
    msg.reply({ Data = JSON.encode(oracle_list) })
  end
)

Logger.info("Pool Process Handlers Loaded.")
