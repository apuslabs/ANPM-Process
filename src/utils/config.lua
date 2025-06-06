-- Configuration for the Pool Process
local M = {}
-- Replace with the actual Pool Manager Process ID during deployment/spawn
M.PoolMgrProcessId = "_Bd_VncRqLzxT9WqY-FZ4N_VwvIWCYl2rsAdPGtXwYg"

-- CreditExchangeRate
M.CreditExchangeRate = "1"
-- APUS TOken Process
M.ApusTokenId = "mqBYxpDsolZmJyBdTK8TJp_ftOuIUXVYcSQ8MYZdJg0"
-- Cost per AI task in credits (as a string for bint_utils)
M.TaskCost = "100000000000000"
-- Treasure wallet address for distribute interest 
M.TreasureWallet = "vtlJ35Z3--epovDI2Cw4swXvsK6PT8h90sfAcx8blQM"
-- InterestGap in milliseconds , 4 minutes = 240000
M.InterestGap = 86400000
-- Config wallet address for dynamic config
M.FeedWallet = "DAPhinKSINxVkHVQQQq4W2WD5KCyqweigMfcJG8TIMM"

-- Database file name
M.DbName = "pool_data.db"

return M
