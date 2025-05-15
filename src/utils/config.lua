-- Configuration for the Pool Process
local M = {}
-- Replace with the actual Pool Manager Process ID during deployment/spawn
M.PoolMgrProcessId = "qSBJv7baBBcgqldPO-6g6Rbw-TSgWfTyAUxm3LohQgI"

-- CreditExchangeRate
M.CreditExchangeRate = "1"
M.CreditsDenomination = "1000000000000"
M.APUSDenomination =  "1000000000000"
-- APUS TOken Process
M.ApusTokenId = "z-g8aooeUGaBX3mNh9ad3MJfVM49YFd_2-Lx9jVqNXw" 
-- Cost per AI task in credits (as a string for bint_utils)
M.TaskCost = "1"

-- Database file name
M.DbName = "pool_data.db"

return M
