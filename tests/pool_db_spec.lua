local luaunit = require('libs.luaunit')
local PoolDAO = require('dao.pool_db')
local sqlite3 = require('lsqlite3')

luaunit.LuaUnit:setOutputType("tap")
luaunit:setVerbosity(luaunit.VERBOSITY_VERBOSE)

TestPoolDAO = {}

function TestPoolDAO:setUp()
    self.poolDAO = PoolDAO.new()
end

function TestPoolDAO:tearDown()
    self.poolDAO.dbAdmin:exec("DELETE FROM tasks")
end

function TestPoolDAO:test_addTask()
    local ref = 1
    local submitter = "ar_address_123"
    local prompt = "Test prompt"
    local config = '{"model": "gpt-4"}'
    
    self.poolDAO:addTask(ref, submitter, prompt, config)
    
    local task = self.poolDAO:getTaskByRef(ref)
    
    luaunit.assertNotNil(task, "Task should exist after adding")
    luaunit.assertEquals(task.ref, ref)
    luaunit.assertEquals(task.submitter, submitter)
    luaunit.assertEquals(task.prompt, prompt)
    luaunit.assertEquals(task.config, config)
    luaunit.assertEquals(task.status, "pending")
    luaunit.assertNil(task.output)
    luaunit.assertNil(task.resolve_node)
end

function TestPoolDAO:test_getAndStartPendingTask()
    self.poolDAO:addTask(1, "submitter1", "prompt1", nil)
    self.poolDAO:addTask(2, "submitter2", "prompt2", nil)
    
    local oracle_node_id = "oracle123"
    local task = self.poolDAO:getAndStartPendingTask(oracle_node_id)
    
    luaunit.assertNotNil(task)
    luaunit.assertEquals(task.ref, 1)
    
    local updated_task = self.poolDAO:getTaskByRef(1)
    luaunit.assertEquals(updated_task.status, "processing")
    luaunit.assertEquals(updated_task.resolve_node, oracle_node_id)
    
    local task2 = self.poolDAO:getTaskByRef(2)
    luaunit.assertEquals(task2.status, "pending")
    
    local task3 = self.poolDAO:getAndStartPendingTask("oracle456")
    luaunit.assertEquals(task3.ref, 2)
end

function TestPoolDAO:test_getAndStartPendingTask_noTasksAvailable()
    local result = self.poolDAO:getAndStartPendingTask("oracle123")
    luaunit.assertNil(result)
end

function TestPoolDAO:test_setTaskResponse()
    local ref = 1
    local oracle_node_id = "oracle123"
    self.poolDAO:addTask(ref, "submitter1", "prompt1", nil)
    self.poolDAO:getAndStartPendingTask(oracle_node_id)
    
    local output = "This is the task output"
    local task, err = self.poolDAO:setTaskResponse(ref, output, oracle_node_id)
    
    luaunit.assertNotNil(task)
    luaunit.assertEquals(err, "")
    
    local updated_task = self.poolDAO:getTaskByRef(ref)
    luaunit.assertEquals(updated_task.status, "done")
    luaunit.assertEquals(updated_task.output, output)
end

function TestPoolDAO:test_setTaskResponse_wrongOracle()
    local ref = 1
    self.poolDAO:addTask(ref, "submitter1", "prompt1", nil)
    self.poolDAO:getAndStartPendingTask("oracle123")
    
    local output = "This is the task output"
    local task, err = self.poolDAO:setTaskResponse(ref, output, "wrong_oracle")
    
    luaunit.assertNil(task)
    luaunit.assertEquals(err, "Oracle mismatch")
    
    local updated_task = self.poolDAO:getTaskByRef(ref)
    luaunit.assertEquals(updated_task.status, "processing")
    luaunit.assertNil(updated_task.output)
end

function TestPoolDAO:test_setTaskResponse_taskNotFound()
    local task, err = self.poolDAO:setTaskResponse(999, "output", "oracle123")
    
    luaunit.assertNil(task)
    luaunit.assertEquals(err, "Task not found")
end

function TestPoolDAO:test_setTaskResponse_taskNotProcessing()
    local ref = 1
    self.poolDAO:addTask(ref, "submitter1", "prompt1", nil)
    
    local task, err = self.poolDAO:setTaskResponse(ref, "output", "oracle123")
    
    luaunit.assertNil(task)
    luaunit.assertEquals(err, "Task not processing")
end

function TestPoolDAO:test_getTaskStatistics()
    self.poolDAO:addTask(1, "submitter1", "prompt1", nil)
    self.poolDAO:addTask(2, "submitter2", "prompt2", nil)
    
    local oracle_node_id = "oracle123"
    self.poolDAO:getAndStartPendingTask(oracle_node_id)
    
    self.poolDAO:addTask(3, "submitter3", "prompt3", nil)
    local second_oracle = "oracle456"
    self.poolDAO:getAndStartPendingTask(second_oracle)
    self.poolDAO:setTaskResponse(2, "output", second_oracle)
    
    local stats = self.poolDAO:getTaskStatistics()
    
    luaunit.assertEquals(stats.total, 3)
    luaunit.assertEquals(stats.pending, 1)
    luaunit.assertEquals(stats.processing, 1)
    luaunit.assertEquals(stats.done, 1)
end

function TestPoolDAO:test_getTaskByRef()
    local ref = 1
    local submitter = "ar_address_123"
    local prompt = "Test prompt"
    self.poolDAO:addTask(ref, submitter, prompt, nil)
    
    local task = self.poolDAO:getTaskByRef(ref)
    
    luaunit.assertNotNil(task)
    luaunit.assertEquals(task.ref, ref)
    luaunit.assertEquals(task.submitter, submitter)
    luaunit.assertEquals(task.prompt, prompt)
    
    local non_existent_task = self.poolDAO:getTaskByRef(999)
    luaunit.assertNil(non_existent_task)
end

luaunit.LuaUnit.run()