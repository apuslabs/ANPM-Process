-- Configuration for the Pool Process
local M = {}

-- Replace with the actual Owner Address during deployment/spawn
M.Owner = "YOUR_POOL_OWNER_ARWEAVE_ADDRESS"

-- Replace with the actual Pool Manager Process ID during deployment/spawn
M.PoolMgrProcessId = "qSBJv7baBBcgqldPO-6g6Rbw-TSgWfTyAUxm3LohQgI"

-- CreditExchangeRate
M.CreditExchangeRate = 1

-- APUS TOken Process
M.ApusTokenId = "aK2fFFBJ1Hilzg_3yeoJuFywZJ8JJXhxMVC3WjBmPkE"
-- Cost per AI task in credits (as a string for bint_utils)
M.TaskCost = "1"

-- Database file name
M.DbName = "pool_data.db"

return M
