// Copyright 2026 The Silkworm Authors
// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>
#include <evmc/evmc.hpp>

#include <silkworm/core/execution/processor.hpp>
#include <silkworm/core/protocol/param.hpp>
#include <silkworm/core/state/in_memory_state.hpp>
#include <silkworm/core/types/account.hpp>
#include <silkworm/core/types/address.hpp>
#include <silkworm/core/types/block.hpp>
#include <silkworm/core/types/evmc_bytes32.hpp>

namespace silkworm {

TEST_CASE("Empty-code contract creation with storage keeps contract incarnation") {
    Block block{};
    block.header.number = 2'664'614;  // Pre-Spurious Dragon: empty-code contracts may remain alive.
    block.header.gas_limit = 1'000'000;
    block.header.beneficiary = 0x61c808d82a3ac53231750dadc13c777b59310bd9_address;

    const evmc::address caller{0x0a6bb546b9208cfab9e8fa2b9b2c042b18df7030_address};
    const evmc::address created{create_address(caller, 0)};

    // Init code: SSTORE(0, 1), then RETURN(0, 0). The resulting account has
    // empty code but is still a contract and its storage must not use incarnation 0.
    Transaction txn{};
    txn.gas_limit = 200'000;
    txn.data = *from_hex("600160005560006000f3");
    txn.odd_y_parity = false;
    txn.r = 1;
    txn.s = 1;
    txn.set_sender(caller);

    InMemoryState state;
    auto rule_set{protocol::rule_set_factory(kMainnetConfig)};
    ExecutionProcessor processor{block, *rule_set, state, kMainnetConfig, true};
    processor.evm().state().add_to_balance(caller, kEther);

    Receipt receipt;
    processor.execute_transaction(txn, receipt);
    REQUIRE(receipt.success);

    processor.evm().state().write_to_db(block.header.number);

    const auto account{state.read_account(created)};
    REQUIRE(account);
    CHECK(account->incarnation == kDefaultIncarnation);
    CHECK(state.read_storage(created, kDefaultIncarnation, {}) == evmc::bytes32{1});
    CHECK(state.read_storage(created, 0, {}) == evmc::bytes32{});
}

}  // namespace silkworm
