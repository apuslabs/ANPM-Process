local config = require('utils.config') -- Assuming config is appropriately loaded based on context

local M = {}

--- Checks if the message sender is the owner defined in config.
-- @param msg The message object containing msg.From
-- @return boolean True if the sender is the owner, false otherwise.
function M.is_owner(msg)
  -- TODO: Need a reliable way to load the correct config for the specific process (Pool or PoolMgr)
  -- This might require passing the owner ID directly or adjusting the require path.
  -- Placeholder: Assume config.Owner is accessible globally or passed in.
  -- local owner_id = require('config').Owner -- This won't work directly across processes
  -- For now, let's assume Owner is loaded into a local variable somehow.
  -- local Owner = "PROCESS_OWNER_ID_PLACEHOLDER" -- Replace with actual loading mechanism
  -- return msg.From == Owner

  -- Simplified approach for now: Assume Owner is globally available via ao.env or similar
  -- In a real scenario, Owner ID would likely be set during spawn or read from config.
  -- Let's assume it's stored in a known variable `Owner` within the process scope.
  return msg.From == Owner -- Requires 'Owner' variable to be defined in the calling scope
end

--- Checks if the message sender is the Pool Manager defined in config.
-- @param msg The message object containing msg.From
-- @return boolean True if the sender is the Pool Manager, false otherwise.
function M.is_pool_mgr(msg)
  -- Similar config loading issue as is_owner
  -- Placeholder: Assume config.PoolMgrProcessId is accessible.
  -- local pool_mgr_id = require('config').PoolMgrProcessId -- This won't work directly
  -- local PoolMgrProcessId = "POOL_MGR_ID_PLACEHOLDER" -- Replace with actual loading mechanism
  -- return msg.From == PoolMgrProcessId

  -- Assume PoolMgrId is loaded into a local variable `PoolMgrId` within the Pool process scope.
   return msg.From == PoolMgrId -- Requires 'PoolMgrId' variable to be defined in the calling scope (Pool process)
end

--- Checks if the message sender is a registered Oracle.
-- @param msg The message object containing msg.From
-- @param oracles The map of registered oracles { [node_id] = owner_address }
-- @return boolean True if the sender is a registered Oracle, false otherwise.
function M.is_oracle(msg, oracles)
  assert(type(oracles) == "table", "Oracles must be a table")
  -- Check if the sender's address is listed as an owner in the oracles map
  for node_id, owner_address in pairs(oracles) do
    if msg.From == owner_address then
      return true
    end
  end
  -- Alternative: Check if msg.From corresponds to a node_id? LLD implies Oracle interaction uses owner address.
  -- Sticking to checking against owner_address as per LLD structure.
  return false
end

return M
