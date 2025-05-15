local sqlite3 = require('lsqlite3')
local DbAdmin = require('utils.db_admin')
local Logger = require('utils.log')
Logger.LogLevel = "trace"
-- Initialize in-memory SQLite database or reuse existing one
PoolDb = PoolDb or sqlite3.open_memory()

local PoolDAO = {}
PoolDAO.__index = PoolDAO

-- Database Initialization
local function initialize_database(db_admin)
  Logger.trace('Initializing Pool database schema...')
  db_admin:exec([[
    CREATE TABLE IF NOT EXISTS tasks (
      ref INTEGER PRIMARY KEY,
      submitter TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending', -- pending, processing, done
      prompt TEXT NOT NULL,
      config TEXT,
      resolve_node TEXT, -- Oracle Node ID (UUID) that took the task
      output TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
  ]])
  -- No separate credit table needed, will use Lua map `Credits` in main process file
  -- No separate oracle table needed, will use Lua map `Oracles` in main process file
  Logger.trace('Pool database schema initialized.')
end

--- Creates a new PoolDAO instance.
-- @param db_path Optional path to the database file. Defaults to in-memory.
-- @return A new PoolDAO instance.
function PoolDAO.new()
  local self = setmetatable({}, PoolDAO)
  self.dbAdmin = DbAdmin.new(PoolDb)
  initialize_database(self.dbAdmin)
  return self
end

-- ===================
-- Task Functions
-- ===================

--- Adds a new task to the database.
-- @param submitter The AR address of the user submitting the task.
-- @param prompt The user's prompt.
-- @param config_str The user's config string (optional).
-- @return The reference ID (ref) of the newly created task.
function PoolDAO:addTask(ref, submitter, prompt, config)
  assert(type(ref) == "number", "Task ref must be a number")
  assert(type(submitter) == "string", "Submitter must be a string")
  assert(type(prompt) == "string", "Prompt must be a string")
  local current_time = math.floor(os.time())
  local sql = [[
    INSERT INTO tasks (ref, submitter, prompt, config, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?);
  ]]
  local params = { ref, submitter, prompt, config, current_time, current_time }
  self.dbAdmin:apply(sql, params)
end

--- Finds the oldest pending task and marks it as processing.
-- @param oracle_node_id The ID of the oracle taking the task.
-- @return The task details table if found and updated, otherwise nil.
function PoolDAO:getAndStartPendingTask(oracle_node_id)
  assert(type(oracle_node_id) == "string", "Oracle Node ID must be a string")
  local current_time = math.floor(os.time())

  -- Find the oldest pending task
  local pending_tasks = self.dbAdmin:select([[
    SELECT * FROM tasks
    WHERE status = 'pending'
    ORDER BY created_at ASC
    LIMIT 1;
  ]], {})

  if not pending_tasks or #pending_tasks == 0 then
    return nil
  end

  local task = pending_tasks[1]

  -- Update the task status to processing
  self.dbAdmin:apply([[
    UPDATE tasks
    SET status = 'processing', resolve_node = ?, updated_at = ?
    WHERE ref = ? AND status = 'pending';
  ]], { oracle_node_id, current_time, task.ref })

  return task
end

function PoolDAO:hasPendingTask()
  -- Find the oldest pending task
  local pending_tasks = self.dbAdmin:select([[
    SELECT ref FROM tasks
    WHERE status = 'pending'
    LIMIT 1;
  ]], {})

  if not pending_tasks or #pending_tasks == 0 then
    return false
  end

  return true
end

--- Updates a task with the response from an Oracle.
-- @param ref The reference ID of the task.
-- @param output The output/result from the Oracle.
-- @param expected_oracle_node_id The Node ID of the Oracle expected to respond.
-- @return The updated task details table if successful, otherwise nil.
function PoolDAO:setTaskResponse(ref, output, expected_oracle_node_id)
  assert(type(ref) == "number", "Task ref must be a number")
  assert(type(output) == "string", "Output must be a string")
  assert(type(expected_oracle_node_id) == "string", "Expected Oracle Node ID must be a string")
  local current_time = math.floor(os.time())

  -- Fetch the task to verify the oracle and status
  local tasks = self.dbAdmin:select([[ SELECT * FROM tasks WHERE ref = ?; ]], { ref })

  if not tasks or #tasks == 0 then
    Logger.error('Task-Response: Task with ref ' .. ref .. ' not found.')
    return nil, "Task not found"
  end

  local task = tasks[1]

  if task.status ~= 'processing' then
     Logger.error('Task-Response: Task ' .. ref .. ' is not in processing state (current: '.. task.status ..').')
     return nil, "Task not processing"
  end

  if task.resolve_node ~= expected_oracle_node_id then
     Logger.error('Task-Response: Task ' .. ref .. ' was assigned to Oracle ' .. task.resolve_node .. ', but response received from ' .. expected_oracle_node_id)
     return nil, "Oracle mismatch"
  end

  -- Update the task
  self.dbAdmin:apply([[
    UPDATE tasks
    SET status = 'done', output = ?, updated_at = ?
    WHERE ref = ? AND status = 'processing'; -- Ensure atomicity
  ]], { output, current_time, ref })

  return task, ""
end

--- Gets statistics about tasks.
-- @return A table with counts for total, pending, processing, and done tasks.
function PoolDAO:getTaskStatistics()
  local sql = [[
    SELECT
      COUNT(*) as total,
      SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
      SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
      SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) as done
    FROM tasks;
  ]]
  local results = self.dbAdmin:select(sql, {})
  if results and #results > 0 then
    return {
      total = results[1].total or 0,
      pending = results[1].pending or 0,
      processing = results[1].processing or 0,
      done = results[1].done or 0
    }
  else
    return { total = 0, pending = 0, processing = 0, done = 0 }
  end
end

--- Gets a specific task by its reference ID.
-- @param ref The task reference ID.
-- @return The task details table or nil if not found.
function PoolDAO:getTaskByRef(ref)
    assert(type(ref) == "number", "Task ref must be a number")
    local sql = [[ SELECT * FROM tasks WHERE ref = ?; ]]
    local tasks = self.dbAdmin:select(sql, { ref })
    if tasks and #tasks > 0 then
        return tasks[1]
    else
        return nil
    end
end


return PoolDAO
