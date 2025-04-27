local sqlite3 = require('lsqlite3')
local DbAdmin = require('utils.db_admin')
local BintUtils = require('utils.bint_utils')
local Logger = require('utils.log')
local config = require('pool.config') -- Use relative path from src

local PoolDb = {}
PoolDb.__index = PoolDb

-- Database Initialization
local function initialize_database(db_admin)
  Logger.log('Initializing Pool database schema...')
  db_admin:exec([[
    CREATE TABLE IF NOT EXISTS tasks (
      ref INTEGER PRIMARY KEY AUTOINCREMENT,
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
  Logger.log('Pool database schema initialized.')
end

--- Creates a new PoolDb instance.
-- @param db_path Optional path to the database file. Defaults to in-memory.
-- @return A new PoolDb instance.
function PoolDb.new(db_path)
  local self = setmetatable({}, PoolDb)
  db_path = db_path or ':memory:' -- Use config.DbName for persistent storage?
  -- For AO processes, direct file system access might be restricted.
  -- Using in-memory DB for now, persistence needs AO state mechanism.
  -- Let's assume DbAdmin handles persistence if needed via AO mechanisms.
  local db = sqlite3.open(config.DbName) -- Use db name from config
  self.dbAdmin = DbAdmin.new(db)
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
function PoolDb:addTask(submitter, prompt, config_str)
  assert(type(submitter) == "string", "Submitter must be a string")
  assert(type(prompt) == "string", "Prompt must be a string")
  local current_time = math.floor(os.time()) -- Use ao.env.Timestamp? Check ao-llms.md
  -- Assuming os.time() is available and suitable for now.
  local sql = [[
    INSERT INTO tasks (submitter, prompt, config, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?);
  ]]
  local params = { submitter, prompt, config_str, current_time, current_time }
  local success, last_id = self.dbAdmin:insert(sql, params)
  if success then
    Logger.log('Task added for submitter ' .. submitter .. ' with ref ' .. last_id)
    return last_id
  else
    Logger.error('Failed to add task for submitter ' .. submitter)
    return nil
  end
end

--- Finds the oldest pending task and marks it as processing.
-- @param oracle_node_id The ID of the oracle taking the task.
-- @return The task details table if found and updated, otherwise nil.
function PoolDb:getAndStartPendingTask(oracle_node_id)
  assert(type(oracle_node_id) == "string", "Oracle Node ID must be a string")
  local current_time = math.floor(os.time())

  -- Find the oldest pending task
  local find_sql = [[
    SELECT * FROM tasks
    WHERE status = 'pending'
    ORDER BY created_at ASC
    LIMIT 1;
  ]]
  local pending_tasks = self.dbAdmin:select(find_sql, {})

  if not pending_tasks or #pending_tasks == 0 then
    Logger.log('No pending tasks found.')
    return nil -- No pending tasks
  end

  local task = pending_tasks[1]

  -- Update the task status to processing
  local update_sql = [[
    UPDATE tasks
    SET status = 'processing', resolve_node = ?, updated_at = ?
    WHERE ref = ? AND status = 'pending'; -- Ensure atomicity
  ]]
  local params = { oracle_node_id, current_time, task.ref }
  local changes = self.dbAdmin:apply(update_sql, params)

  if changes > 0 then
    Logger.log('Task ' .. task.ref .. ' assigned to Oracle ' .. oracle_node_id)
    -- Return the task details after update attempt
    local updated_task_sql = [[ SELECT * FROM tasks WHERE ref = ?; ]]
    local updated_tasks = self.dbAdmin:select(updated_task_sql, { task.ref })
    return updated_tasks[1]
  else
    Logger.warn('Failed to update task ' .. task.ref .. ' status to processing (maybe already taken).')
    return nil -- Task might have been taken by another oracle concurrently
  end
end

--- Updates a task with the response from an Oracle.
-- @param ref The reference ID of the task.
-- @param output The output/result from the Oracle.
-- @param expected_oracle_node_id The Node ID of the Oracle expected to respond.
-- @return The updated task details table if successful, otherwise nil.
function PoolDb:setTaskResponse(ref, output, expected_oracle_node_id)
  assert(type(ref) == "number", "Task ref must be a number")
  assert(type(output) == "string", "Output must be a string")
  assert(type(expected_oracle_node_id) == "string", "Expected Oracle Node ID must be a string")
  local current_time = math.floor(os.time())

  -- Fetch the task to verify the oracle and status
  local get_sql = [[ SELECT * FROM tasks WHERE ref = ?; ]]
  local tasks = self.dbAdmin:select(get_sql, { ref })

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
  local update_sql = [[
    UPDATE tasks
    SET status = 'done', output = ?, updated_at = ?
    WHERE ref = ? AND status = 'processing'; -- Ensure atomicity
  ]]
  local params = { output, current_time, ref }
  local changes = self.dbAdmin:apply(update_sql, params)

  if changes > 0 then
    Logger.log('Task ' .. ref .. ' marked as done.')
    -- Return the updated task details
    local updated_task_sql = [[ SELECT * FROM tasks WHERE ref = ?; ]]
    local updated_tasks = self.dbAdmin:select(updated_task_sql, { ref })
    return updated_tasks[1], nil
  else
     Logger.error('Failed to update task ' .. ref .. ' to done state.')
     return nil, "Update failed"
  end
end

--- Gets statistics about tasks.
-- @return A table with counts for total, pending, processing, and done tasks.
function PoolDb:getTaskStatistics()
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
function PoolDb:getTaskByRef(ref)
    assert(type(ref) == "number", "Task ref must be a number")
    local sql = [[ SELECT * FROM tasks WHERE ref = ?; ]]
    local tasks = self.dbAdmin:select(sql, { ref })
    if tasks and #tasks > 0 then
        return tasks[1]
    else
        return nil
    end
end


return PoolDb
