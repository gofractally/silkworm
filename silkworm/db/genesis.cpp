// Copyright 2025 The Silkworm Authors
// SPDX-License-Identifier: Apache-2.0

#include "genesis.hpp"

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <vector>

#include <silkworm/core/chain/genesis.hpp>
#include <silkworm/core/common/assert.hpp>
#include <silkworm/core/common/empty_hashes.hpp>
#include <silkworm/core/trie/nibbles.hpp>
#include <silkworm/core/types/address.hpp>

#include "state/account_codec.hpp"
#include "tables.hpp"

namespace silkworm::db {

using datastore::kvdb::to_slice;

std::pair<bool, std::vector<std::string>> validate_genesis_json(const nlohmann::json& genesis_json) {
    std::pair<bool, std::vector<std::string>> ret{true, {}};
    if (genesis_json.is_discarded()) {
        ret.second.emplace_back("Invalid json data");
    } else {
        if (!genesis_json.contains("difficulty")) {
            ret.second.emplace_back("Missing difficulty member");
        } else {
            auto str{genesis_json["difficulty"].get<std::string>()};
            auto bytes = from_hex(str);
            if (!bytes.has_value()) {
                ret.second.emplace_back("Member difficulty is not a valid hex");
            }
        }
        if (!genesis_json.contains("gasLimit")) ret.second.emplace_back("Missing gasLimit member");
        if (!genesis_json.contains("timestamp")) ret.second.emplace_back("Missing timestamp member");
        if (!genesis_json.contains("config")) {
            ret.second.emplace_back("Missing config member");
        } else {
            if (!genesis_json["config"].is_object()) {
                ret.second.emplace_back("Member config is not object");
            } else {
                auto& genesis_config_json{genesis_json["config"]};
                const auto chain_config = ChainConfig::from_json(genesis_config_json);
                if (!chain_config.has_value()) {
                    ret.second.emplace_back("Incomplete / Wrong genesis config member");
                } else {
                    if (std::holds_alternative<protocol::EthashConfig>(chain_config->rule_set_config)) {
                        if (!genesis_json.contains("mixHash") || !genesis_json["mixHash"].is_string() ||
                            !genesis_json.contains("nonce") || !genesis_json["nonce"].is_string()) {
                            ret.second.emplace_back("Missing mixHash and or nonce member for ethash PoW chain");
                        } else {
                            auto mixhash = from_hex(genesis_json["mixHash"].get<std::string>());
                            if (!mixhash.has_value() || mixhash->size() != kHashLength) {
                                ret.second.emplace_back("mixHash member is not a valid hash hex string");
                            }
                            auto nonce = from_hex(genesis_json["nonce"].get<std::string>());
                            if (!nonce.has_value()) {
                                ret.second.emplace_back("nonce member is not a valid hex string");
                            }
                        }
                    }
                }
            }
        }

        if (genesis_json.contains("alloc")) {
            if (!genesis_json["alloc"].is_object()) {
                ret.second.emplace_back("alloc member is not object");
            } else {
                // Check for sanity of allocations
                // NOTE ! There is no need to check for uniqueness of keys as,
                // being an object, keys are already unique (otherwise parsing of Json fails)
                for (auto& item : genesis_json["alloc"].items()) {
                    if (!item.value().is_object() || !item.value().contains("balance") ||
                        !item.value()["balance"].is_string()) {
                        ret.second.emplace_back("Allocation for  " + item.key() + " is badly formatted");
                        continue;
                    }

                    auto address_bytes{from_hex(item.key())};
                    if (!address_bytes.has_value() || address_bytes->size() != kAddressLength) {
                        ret.second.emplace_back("Allocation for " + item.key() + " has invalid address");
                        continue;
                    }

                    try {
                        auto balance_str{item.value()["balance"].get<std::string>()};
                        (void)intx::from_string<intx::uint256>(balance_str);
                    } catch (const std::exception&) {
                        ret.second.emplace_back("Allocation for " + item.key() + " has bad balance");
                    }
                }
            }
        }
    }

    ret.first = ret.second.empty();
    return ret;
}

evmc::bytes32 initialize_genesis_allocations(RWTxn& txn, const nlohmann::json& genesis_json) {
    InMemoryState state{read_genesis_allocation(genesis_json.at("alloc"))};
    write_genesis_allocation_to_db(txn, state);
    return state.state_root_hash();
}

void write_genesis_allocation_to_db(RWTxn& txn, const InMemoryState& genesis_allocation) {
    const bool optimized_plain_state{table::use_psitri_optimized_plain_state()};
    std::unique_ptr<datastore::kvdb::RWCursor> state_table;
    if (optimized_plain_state) {
        state_table = txn.rw_cursor(table::plain_state_config());
    } else {
        state_table = txn.rw_cursor_dup_sort(table::plain_state_config());
    }
    auto* state_table_dup = optimized_plain_state ? nullptr : dynamic_cast<datastore::kvdb::RWCursorDupSort*>(state_table.get());
    if (!optimized_plain_state && !state_table_dup) {
        throw std::logic_error("PlainState cursor does not support multivalue operations");
    }
    auto code_table{open_cursor(txn, table::kCode)};

    std::vector<evmc::address> account_addresses;
    account_addresses.reserve(genesis_allocation.accounts().size());
    for (const auto& [address, _] : genesis_allocation.accounts()) {
        account_addresses.push_back(address);
    }
    std::sort(account_addresses.begin(), account_addresses.end());

    for (const auto& address : account_addresses) {
        const auto account_it = genesis_allocation.accounts().find(address);
        SILKWORM_ASSERT(account_it != genesis_allocation.accounts().end());
        const auto& account = account_it->second;

        // Store account plain state
        Bytes encoded = state::AccountCodec::encode_for_storage(account);
        state_table->upsert(to_slice(address), to_slice(encoded));

        // Store code
        if (account.code_hash != kEmptyHash) {
            ByteView code{genesis_allocation.read_code(address, account.code_hash)};
            code_table.upsert(to_slice(account.code_hash), to_slice(code));
        }
    }

    std::vector<evmc::address> storage_addresses;
    storage_addresses.reserve(genesis_allocation.storage().size());
    for (const auto& [address, _] : genesis_allocation.storage()) {
        storage_addresses.push_back(address);
    }
    std::sort(storage_addresses.begin(), storage_addresses.end());

    for (const auto& address : storage_addresses) {
        const auto incarnations_it = genesis_allocation.storage().find(address);
        SILKWORM_ASSERT(incarnations_it != genesis_allocation.storage().end());
        const auto& incarnations = incarnations_it->second;

        std::vector<uint64_t> incarnation_keys;
        incarnation_keys.reserve(incarnations.size());
        for (const auto& [incarnation, _] : incarnations) {
            incarnation_keys.push_back(incarnation);
        }
        std::sort(incarnation_keys.begin(), incarnation_keys.end());

        for (const auto incarnation : incarnation_keys) {
            const auto storage_it = incarnations.find(incarnation);
            SILKWORM_ASSERT(storage_it != incarnations.end());
            const auto& storage = storage_it->second;

            Bytes prefix;
            if (!optimized_plain_state) {
                prefix = storage_prefix(address, incarnation);
            }
            std::vector<evmc::bytes32> locations;
            locations.reserve(storage.size());
            for (const auto& [location, _] : storage) {
                locations.push_back(location);
            }
            std::sort(locations.begin(), locations.end());

            for (const auto& location : locations) {
                const auto value_it = storage.find(location);
                SILKWORM_ASSERT(value_it != storage.end());
                const auto& value = value_it->second;
                if (optimized_plain_state) {
                    upsert_flat_storage_value(*state_table, address, incarnation, location.bytes, value.bytes);
                } else {
                    upsert_storage_value(*state_table_dup, prefix, location.bytes, value.bytes);
                }
            }
        }
    }
}

bool initialize_genesis(RWTxn& txn, const nlohmann::json& genesis_json, bool allow_exceptions) {
    if (!txn->is_readwrite()) {
        if (!allow_exceptions) {
            return false;
        }
        throw std::runtime_error("Unable to write to db with a RO transaction");
    }

    auto existing_config{read_chain_config(txn)};
    if (existing_config.has_value()) {
        if (!allow_exceptions) {
            return false;
        }
        throw std::runtime_error("This database is already initialized with genesis");
    }

    // Validate json payload
    auto [valid, errors]{validate_genesis_json(genesis_json)};
    if (!valid) {
        if (!allow_exceptions) {
            return false;
        }
        const char* delim{"\n"};
        std::ostringstream imploded;
        std::copy(errors.begin(), errors.end(), std::ostream_iterator<std::string>(imploded, delim));
        throw std::runtime_error("Invalid genesis json payload. Examine following errors:\n" + imploded.str());
    }

    try {
        // Allocate accounts
        const evmc::bytes32 state_root_hash{initialize_genesis_allocations(txn, genesis_json)};

        const BlockHeader header{read_genesis_header(genesis_json, state_root_hash)};

        auto block_hash{header.hash()};
        auto block_hash_key{block_key(header.number, block_hash.bytes)};
        write_header(txn, header, /*with_header_numbers=*/true);            // Write table::kHeaders and table::kHeaderNumbers
        write_canonical_header_hash(txn, block_hash.bytes, header.number);  // Insert header hash as canonical
        write_total_difficulty(txn, block_hash_key, header.difficulty);     // Write initial difficulty

        write_body(txn, BlockBody(), block_hash.bytes, header.number);  // Write block body (empty)
        write_head_header_hash(txn, block_hash.bytes);                  // Update head header in config

        // TODO(Andrea) verify how receipts are stored (see buffer.cpp)
        const uint8_t genesis_null_receipts[] = {0xf6};  // <- cbor encoded
        open_cursor(txn, table::kBlockReceipts)
            .upsert(to_slice(block_hash_key).safe_middle(0, 8), to_slice(Bytes(genesis_null_receipts, 1)));

        // Write Chain Settings
        auto config_data{genesis_json["config"].dump()};
        open_cursor(txn, table::kConfig)
            .upsert(to_slice(block_hash), mdbx::slice{config_data.data()});

        return true;

    } catch (const std::exception&) {
        if (!allow_exceptions) {
            return false;
        }
        throw;
    }
}

}  // namespace silkworm::db
