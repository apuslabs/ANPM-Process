-- Configuration for the Pool Process
local M = {}
-- Replace with the actual Pool Manager Process ID during deployment/spawn
M.PoolMgrProcessId = "_Bd_VncRqLzxT9WqY-FZ4N_VwvIWCYl2rsAdPGtXwYg"

-- CreditExchangeRate
M.CreditExchangeRate = "1"
M.CreditsDenomination = "1000000000000"
M.APUSDenomination = "1000000000000"
-- APUS TOken Process
M.ApusTokenId = "5HTh33IQm5Ju3h9x3jKL5GdyPdmVWOGR4HA9x5NZX8U"
-- Cost per AI task in credits (as a string for bint_utils)
M.TaskCost = "1"
M.TreasureWallet = "69opDSKypKRhg8uW1vzAwCQch55F8owiMlyxEKYMnrU"
-- Database file name
M.DbName = "pool_data.db"

return M
