-- Configuration for the Pool Manager Process
local M = {}

-- Replace with the actual Owner Address during deployment/spawn
M.Owner = "YOUR_POOL_MANAGER_OWNER_ARWEAVE_ADDRESS"

-- Default exchange rate: 1000 APUS = 1000 Credits (adjust as needed)
-- Represents how many credits are given per 1 unit of APUS (assuming APUS smallest unit)
M.CreditsPerApus = "1" -- Stored as string for bint_utils

-- Database file name
M.DbName = "pool_mgr_data.db"

-- APUS Token Process ID (replace with actual)
M.ApusTokenId = "APUS_TOKEN_PROCESS_ID"

-- Default Staking Capacity per Pool (can be overridden on pool creation) - Example value
M.DefaultPoolStakingCapacity = "1000000" -- String for bint

-- Default Daily Rewards per Pool (can be overridden on pool creation) - Example value
M.DefaultPoolRewardsAmount = "1000" -- String for bint

return M
