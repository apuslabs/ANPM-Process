-- Configuration for the Pool Process
local M = {}

-- Replace with the actual Owner Address during deployment/spawn
M.Owner = "YOUR_POOL_OWNER_ARWEAVE_ADDRESS"

-- Replace with the actual Pool Manager Process ID during deployment/spawn
M.PoolMgrProcessId = "YOUR_POOL_MANAGER_PROCESS_ID"

-- Cost per AI task in credits (as a string for bint_utils)
M.TaskCost = "1"

-- Database file name
M.DbName = "pool_data.db"

return M
