
local luaunit = require('libs.luaunit')
local PoolMgrDAO = require('dao.pool_mgr_db') 
local BintUtils = require('utils.bint_utils') 

luaunit.LuaUnit:setOutputType("tap")
luaunit:setVerbosity(luaunit.VERBOSITY_VERBOSE)

TestPoolMgrDAO = {}

function TestPoolMgrDAO:setUp()
    self.poolMgrDAO = PoolMgrDAO.new()
    -- Mock os.time for consistent created_at timestamps if necessary for specific assertions
    -- For now, we'll rely on the order of insertion for sequences.
    self.original_os_time = os.time
    self.mock_time_counter = 1678886400 -- A fixed starting point for time (e.g., 2023-03-15 12:00:00 UTC)
    os.time = function()
        self.mock_time_counter = self.mock_time_counter + 1
        return self.mock_time_counter
    end
end

function TestPoolMgrDAO:tearDown()
    self.poolMgrDAO.dbAdmin:exec("DELETE FROM credits_records")
    self.poolMgrDAO.dbAdmin:exec("DELETE FROM user_staking_transactions")
    os.time = self.original_os_time -- Restore original os.time
end

-- ===================
-- Credit Functions Tests
-- ===================

function TestPoolMgrDAO:test_recordCreditTransaction_and_getUserCreditsRecords()
    local ref1 = 1001
    local wallet1 = "wallet_address_credits_1"
    local action1 = "buy"
    local quantity1 = "1000"
    local pool_id1 = "0"
    local time_before_record1 = os.time()

    self.poolMgrDAO:recordCreditTransaction(ref1, wallet1, action1, quantity1, pool_id1)


    local records_w1 = self.poolMgrDAO:getUserCreditsRecords(wallet1)
    luaunit.assertEquals(#records_w1, 1, "Should be one record for wallet1")
    local rec1 = records_w1[1]
    luaunit.assertEquals(rec1.ref, ref1)
    luaunit.assertEquals(rec1.wallet_address, wallet1)
    luaunit.assertEquals(rec1.action, action1)
    luaunit.assertEquals(rec1.quantity, quantity1)
    luaunit.assertEquals(rec1.pool_id, pool_id1)
    -- Check timestamp; os.time() inside recordCreditTransaction is called once.
    luaunit.assertEquals(rec1.created_at, time_before_record1 + 1, "Timestamp should be the mocked time of insertion")


    local ref2 = 1002
    local wallet2 = "wallet_address_credits_2"
    local action2 = "add"
    local quantity2 = "500"
    local pool_id2 = "pool_A"
    self.poolMgrDAO:recordCreditTransaction(ref2, wallet2, action2, quantity2, pool_id2)

    local ref3 = 1003
    local action3 = "transfer"
    local quantity3 = "200"
    local pool_id3_source = "pool_A" 
    self.poolMgrDAO:recordCreditTransaction(ref3, wallet1, action3, quantity3, pool_id3_source)


    records_w1 = self.poolMgrDAO:getUserCreditsRecords(wallet1)
    luaunit.assertEquals(#records_w1, 2, "Should be two records for wallet1")
    luaunit.assertEquals(records_w1[2].action, action3)
    luaunit.assertEquals(records_w1[2].quantity, quantity3)

    local records_w2 = self.poolMgrDAO:getUserCreditsRecords(wallet2)
    luaunit.assertEquals(#records_w2, 1, "Should be one record for wallet2")
    luaunit.assertEquals(records_w2[1].ref, ref2)

    local no_records = self.poolMgrDAO:getUserCreditsRecords("non_existent_wallet")
    luaunit.assertEquals(#no_records, 0, "Should be zero records for non_existent_wallet")
end

function TestPoolMgrDAO:test_getAllCreditsRecords()
    local all_records_empty = self.poolMgrDAO:getAllCreditsRecords()
    luaunit.assertEquals(#all_records_empty, 0, "Should be zero records initially")

    local t1 = os.time() + 1
    self.poolMgrDAO:recordCreditTransaction(2001, "w1", "buy", "100", "0")
    local t2 = os.time() + 1
    self.poolMgrDAO:recordCreditTransaction(2002, "w2", "add", "200", "P1")
    local t3 = os.time() + 1
    self.poolMgrDAO:recordCreditTransaction(2003, "w1", "transfer", "50", "P2")

    local all_records = self.poolMgrDAO:getAllCreditsRecords()
    luaunit.assertEquals(#all_records, 3, "Should be three records in total")
    -- Check order (DESC by created_at)
    luaunit.assertEquals(all_records[1].ref, 2003)
    luaunit.assertEquals(all_records[1].created_at, t3)
    luaunit.assertEquals(all_records[2].ref, 2002)
    luaunit.assertEquals(all_records[2].created_at, t2)
    luaunit.assertEquals(all_records[3].ref, 2001)
    luaunit.assertEquals(all_records[3].created_at, t1)
end




-- ===================
-- Staking Functions Tests
-- ===================

function TestPoolMgrDAO:test_recordStakingTransaction_and_getCurrentStake()
    local user1 = "user_stake_1"
    local pool1 = "pool_S1"
    local amount1_stake = "1000"
    
    self.poolMgrDAO:recordStakingTransaction(user1, pool1, "STAKE", amount1_stake)

    local current_stake_u1p1 = self.poolMgrDAO:getCurrentStake(user1, pool1)
    luaunit.assertEquals(current_stake_u1p1, amount1_stake)
    
    local amount1_unstake = "300"
    self.poolMgrDAO:recordStakingTransaction(user1, pool1, "UNSTAKE", amount1_unstake)
    current_stake_u1p1 = self.poolMgrDAO:getCurrentStake(user1, pool1)
    luaunit.assertEquals(current_stake_u1p1, BintUtils.sub(amount1_stake, amount1_unstake)) -- "700"

    local user2 = "user_stake_2"
    local pool2 = "pool_S2"
    local amount2_stake = "5000"
    self.poolMgrDAO:recordStakingTransaction(user2, pool2, "STAKE", amount2_stake)
    local current_stake_u2p2 = self.poolMgrDAO:getCurrentStake(user2, pool2)
    luaunit.assertEquals(current_stake_u2p2, amount2_stake)

    local amount1_stake_more = "200"
    self.poolMgrDAO:recordStakingTransaction(user1, pool1, "STAKE", amount1_stake_more)
    current_stake_u1p1 = self.poolMgrDAO:getCurrentStake(user1, pool1)
    luaunit.assertEquals(current_stake_u1p1, BintUtils.add(BintUtils.sub(amount1_stake, amount1_unstake), amount1_stake_more)) -- "900"

    local current_stake_u1_nonexistent_pool = self.poolMgrDAO:getCurrentStake(user1, "non_existent_pool")
    luaunit.assertEquals(current_stake_u1_nonexistent_pool, "0")

    local current_stake_nonexistent_user_p1 = self.poolMgrDAO:getCurrentStake("non_existent_user", pool1)
    luaunit.assertEquals(current_stake_nonexistent_user_p1, "0")
end

function TestPoolMgrDAO:test_getTotalUserStake()
    local user1 = "total_stake_user1"
    local poolA = "pool_TS_A"
    local poolB = "pool_TS_B"

    local total_stake_u1_empty = self.poolMgrDAO:getTotalUserStake(user1)
    luaunit.assertEquals(total_stake_u1_empty, "0")

    self.poolMgrDAO:recordStakingTransaction(user1, poolA, "STAKE", "100")
    local total_stake_u1 = self.poolMgrDAO:getTotalUserStake(user1)
    luaunit.assertEquals(total_stake_u1, "100")

    self.poolMgrDAO:recordStakingTransaction(user1, poolB, "STAKE", "250")
    total_stake_u1 = self.poolMgrDAO:getTotalUserStake(user1)
    luaunit.assertEquals(total_stake_u1, BintUtils.add("100", "250")) -- "350"

    self.poolMgrDAO:recordStakingTransaction(user1, poolA, "UNSTAKE", "50")
    total_stake_u1 = self.poolMgrDAO:getTotalUserStake(user1)
    luaunit.assertEquals(total_stake_u1, BintUtils.sub(BintUtils.add("100", "250"), "50")) -- "300"

    self.poolMgrDAO:recordStakingTransaction(user1, poolA, "STAKE", "150")
    total_stake_u1 = self.poolMgrDAO:getTotalUserStake(user1)
    luaunit.assertEquals(total_stake_u1, BintUtils.add(BintUtils.add(BintUtils.sub("100","50"), "150"), "250")) -- "450"

    local user2 = "total_stake_user2"
    self.poolMgrDAO:recordStakingTransaction(user2, poolA, "STAKE", "700")
    local total_stake_u2 = self.poolMgrDAO:getTotalUserStake(user2)
    luaunit.assertEquals(total_stake_u2, "700")

    total_stake_u1 = self.poolMgrDAO:getTotalUserStake(user1)
    luaunit.assertEquals(total_stake_u1, "450")

    local total_stake_nonexistent = self.poolMgrDAO:getTotalUserStake("non_existent_user_for_total")
    luaunit.assertEquals(total_stake_nonexistent, "0")
end

function TestPoolMgrDAO:test_getTotalPoolStake()
    local poolX = "pool_TPS_X"
    local poolY = "pool_TPS_Y"
    local userA = "user_tps_A"
    local userB = "user_tps_B"

    local total_stake_px_empty = self.poolMgrDAO:getTotalPoolStake(poolX)
    luaunit.assertEquals(total_stake_px_empty, "0")

    self.poolMgrDAO:recordStakingTransaction(userA, poolX, "STAKE", "300")
    local total_stake_px = self.poolMgrDAO:getTotalPoolStake(poolX)
    luaunit.assertEquals(total_stake_px, "300")

    self.poolMgrDAO:recordStakingTransaction(userB, poolX, "STAKE", "400")
    total_stake_px = self.poolMgrDAO:getTotalPoolStake(poolX)
    luaunit.assertEquals(total_stake_px, BintUtils.add("300", "400")) -- "700"

    self.poolMgrDAO:recordStakingTransaction(userA, poolX, "UNSTAKE", "100")
    total_stake_px = self.poolMgrDAO:getTotalPoolStake(poolX)
    luaunit.assertEquals(total_stake_px, BintUtils.sub(BintUtils.add("300", "400"), "100")) -- "600"

    self.poolMgrDAO:recordStakingTransaction(userA, poolY, "STAKE", "50")
    local total_stake_py = self.poolMgrDAO:getTotalPoolStake(poolY)
    luaunit.assertEquals(total_stake_py, "50")

    total_stake_px = self.poolMgrDAO:getTotalPoolStake(poolX)
    luaunit.assertEquals(total_stake_px, "600")

    local total_stake_nonexistent = self.poolMgrDAO:getTotalPoolStake("non_existent_pool_for_total")
    luaunit.assertEquals(total_stake_nonexistent, "0")
end

function TestPoolMgrDAO:test_staking_edge_cases_zero_and_large_amounts()
    local user = "user_edge"
    local pool = "pool_edge"

    self.poolMgrDAO:recordStakingTransaction(user, pool, "STAKE", "0")
    local stake = self.poolMgrDAO:getCurrentStake(user, pool)
    luaunit.assertEquals(stake, "0")

    local large_amount = "123456789012345678901234567890"
    self.poolMgrDAO:recordStakingTransaction(user, pool, "STAKE", large_amount)
    stake = self.poolMgrDAO:getCurrentStake(user, pool)
    luaunit.assertEquals(stake, large_amount)

    local unstake_amount = "123456789012345678900000000000"
    local expected_after_unstake = BintUtils.sub(large_amount, unstake_amount)
    self.poolMgrDAO:recordStakingTransaction(user, pool, "UNSTAKE", unstake_amount)
    stake = self.poolMgrDAO:getCurrentStake(user, pool)
    luaunit.assertEquals(stake, expected_after_unstake)
    
    self.poolMgrDAO:recordStakingTransaction(user, "pool_over_unstake", "STAKE", "100")
    self.poolMgrDAO:recordStakingTransaction(user, "pool_over_unstake", "UNSTAKE", "200")
    stake = self.poolMgrDAO:getCurrentStake(user, "pool_over_unstake")
    luaunit.assertEquals(stake, BintUtils.sub("100", "200")) -- Assumes BintUtils.sub can return negative
end

-- Note on potential issue in pool_mgr_db.lua:
-- The `user_staking_transactions` table schema currently is:
--   (id, user_wallet_address, pool_id, amount, created_at)
-- The `recordStakingTransaction` function in `pool_mgr_db.lua` attempts to insert using these SQL parameters:
--   (user_wallet_address, pool_id, transaction_type, amount, created_at)
-- This causes a mismatch: `transaction_type` (e.g., 'STAKE') goes into the `amount` column,
-- and the actual `amount` (e.g., '100') goes into the `created_at` column.
--
-- The staking query functions (`getCurrentStake`, etc.) correctly expect `transaction_type` and `amount` columns.
--
-- **Required Fix in `pool_mgr_db.lua`:**
-- The `user_staking_transactions` table schema in `initialize_database` function should be:
--   CREATE TABLE IF NOT EXISTS user_staking_transactions (
--     id INTEGER PRIMARY KEY AUTOINCREMENT,
--     user_wallet_address TEXT NOT NULL,
--     pool_id TEXT NOT NULL,
--     transaction_type TEXT NOT NULL, -- This column needs to be added
--     amount TEXT NOT NULL DEFAULT '0',
--     created_at INTEGER NOT NULL
--   );
-- The tests above are written assuming this correction has been made to `pool_mgr_db.lua`.
--
-- A mock `utils/bint.lua` is also assumed for testing:
-- ```lua
-- -- utils/bint.lua (minimal mock for testing)
-- local BintUtils = {}
-- function BintUtils.add(a, b) return tostring(tonumber(a) + tonumber(b)) end
-- function BintUtils.sub(a, b) return tostring(tonumber(a) - tonumber(b)) end
-- return BintUtils
-- ```

luaunit.LuaUnit.run()