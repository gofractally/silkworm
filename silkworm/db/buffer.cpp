// Copyright 2025 The Silkworm Authors
// SPDX-License-Identifier: Apache-2.0

#include "buffer.hpp"

#include <algorithm>
#include <cstring>
#include <memory>
#include <stdexcept>

#include <absl/container/btree_set.h>

#include <silkworm/core/common/endian.hpp>
#include <silkworm/db/access_layer.hpp>
#include <silkworm/db/log_cbor.hpp>
#include <silkworm/db/receipt_cbor.hpp>
#include <silkworm/db/state/account_codec.hpp>
#include <silkworm/db/tables.hpp>
#include <silkworm/infra/common/decoding_exception.hpp>
#include <silkworm/infra/common/log.hpp>
#include <silkworm/infra/common/stopwatch.hpp>

namespace silkworm::db {

using datastore::kvdb::to_slice;

namespace {

bool is_traced_bad_gas_address(const evmc::address& address) {
    static constexpr uint8_t kAddress[]{
        0x60, 0x62, 0xe4, 0x66, 0xcf, 0x33, 0xa5, 0xd1, 0xe2, 0x2a,
        0xc5, 0x7b, 0x2a, 0x72, 0x6a, 0x23, 0xbf, 0x79, 0xa0, 0xd0};
    return std::memcmp(address.bytes, kAddress, sizeof(kAddress)) == 0;
}

bool is_traced_bad_gas_code_hash(const evmc::bytes32& code_hash) {
    static constexpr uint8_t kCodeHash[]{
        0x24, 0x20, 0xae, 0x84, 0xd6, 0xa3, 0xb3, 0x64,
        0x42, 0x08, 0x2a, 0x6e, 0xc0, 0xb5, 0x52, 0xb4,
        0xdb, 0xe8, 0xbc, 0x64, 0xdd, 0x2a, 0x89, 0xa7,
        0x2c, 0x4c, 0xa3, 0x2e, 0x64, 0x05, 0x56, 0x03};
    return std::memcmp(code_hash.bytes, kCodeHash, sizeof(kCodeHash)) == 0;
}

}  // namespace

void Buffer::reset_cached_cursors() const noexcept {
    plain_state_cursor_.reset();
    plain_code_hash_cursor_.reset();
    cached_cursor_txn_id_.reset();
}

void Buffer::ensure_cached_cursors_current() const {
    const uint64_t txn_id{txn_.id()};
    if (cached_cursor_txn_id_ == txn_id) {
        return;
    }

    plain_state_cursor_.reset();
    plain_code_hash_cursor_.reset();
    cached_cursor_txn_id_ = txn_id;
}

datastore::kvdb::ROCursor& Buffer::plain_state_cursor() const {
    ensure_cached_cursors_current();
    if (!plain_state_cursor_) {
        plain_state_cursor_ = txn_.ro_cursor(table::plain_state_config());
    }
    return *plain_state_cursor_;
}

datastore::kvdb::ROCursor& Buffer::plain_code_hash_cursor() const {
    ensure_cached_cursors_current();
    if (!plain_code_hash_cursor_) {
        plain_code_hash_cursor_ = txn_.ro_cursor(table::kPlainCodeHash);
    }
    return *plain_code_hash_cursor_;
}

template <class TFlatHashMap>
size_t flat_hash_map_memory_size(size_t capacity) {
    return sizeof(std::pair<const typename TFlatHashMap::key_type, typename TFlatHashMap::mapped_type>) * capacity;
}

static size_t flat_hash_map_capacity_for_size(size_t size, size_t current_capacity) {
    // if the desired size is less than the growth threshold, the current capacity is enough
    if (size * uint64_t{32} <= current_capacity * uint64_t{25}) {
        return current_capacity;
    }
    // otherwise the capacity needs to double up
    return current_capacity * 2;
}

template <class TFlatHashMap>
size_t flat_hash_map_memory_size_after_inserts(const TFlatHashMap& map, size_t inserts_count) {
    size_t capacity_after_inserts = flat_hash_map_capacity_for_size(map.size() + inserts_count, map.capacity());
    return flat_hash_map_memory_size<TFlatHashMap>(capacity_after_inserts);
}

static void replace_estimated_size(size_t& total, size_t old_size, size_t new_size) noexcept {
    if (new_size >= old_size) {
        total += new_size - old_size;
    } else {
        total -= old_size - new_size;
    }
}

void Buffer::begin_block(uint64_t block_num, size_t updated_accounts_count) {
    if (current_batch_size() > memory_limit_) {
        throw MemoryLimitError();
    }
    if (flat_hash_map_memory_size_after_inserts(accounts_, updated_accounts_count) > memory_limit_) {
        throw MemoryLimitError();
    }

    block_num_ = block_num;
    changed_storage_.clear();
}

void Buffer::update_account(const evmc::address& address, std::optional<Account> initial,
                            std::optional<Account> current) {
    // Skip update if both initial and final state are non-existent (i.e. contract creation+destruction within the same block)
    if (!initial && !current) {
        // Only to perfectly match Erigon state batch size (Erigon does count any account w/ old=new=empty value).
        batch_state_size_ += kAddressLength;
        return;
    }

    const bool equal{current == initial};
    const bool account_deleted{!current.has_value()};

    if (equal && !account_deleted && !changed_storage_.contains(address)) {
        // Follows the Erigon logic when to populate account changes.
        // See (ChangeSetWriter)UpdateAccountData & DeleteAccount.
        return;
    }

    if (block_num_ >= prune_history_threshold_) {
        Bytes encoded_initial{};
        if (initial) {
            bool omit_code_hash{!account_deleted};
            encoded_initial = state::AccountCodec::encode_for_storage(*initial, omit_code_hash);
        }

        auto& account_changes{block_account_changes_[block_num_]};
        const size_t estimated_size{sizeof(BlockNum) + kAddressLength + encoded_initial.size()};
        if (auto it{account_changes.find(address)}; it != account_changes.end()) {
            replace_estimated_size(batch_history_size_, sizeof(BlockNum) + kAddressLength + it->second.size(), estimated_size);
            it->second = std::move(encoded_initial);
        } else {
            batch_history_size_ += estimated_size;
            account_changes.emplace(address, std::move(encoded_initial));
        }
    }

    size_t encoding_length_for_storage = current ? state::AccountCodec::encoding_length_for_storage(*current) : 0;

    if (equal) {
        batch_state_size_ += kAddressLength + encoding_length_for_storage;
        return;
    }

    auto it{accounts_.find(address)};
    if (it != accounts_.end()) {
        batch_state_size_ -= it->second.has_value() ? state::AccountCodec::encoding_length_for_storage(*it->second) : 0;
        batch_state_size_ += encoding_length_for_storage;
        it->second = current;
    } else {
        batch_state_size_ += kAddressLength + encoding_length_for_storage;
        accounts_[address] = current;
    }

    if (is_traced_bad_gas_address(address)) {
        SILK_ERROR_M("PsiTriStateTrace",
                     {"write", "update-account",
                      "address", to_hex(address.bytes, true),
                      "present", current ? "1" : "0",
                      "nonce", current ? std::to_string(current->nonce) : "",
                      "incarnation", current ? std::to_string(current->incarnation) : "",
                      "code_hash", current ? to_hex(current->code_hash.bytes, true) : ""});
    }

    const bool initial_smart_now_deleted{account_deleted && initial->incarnation};
    const bool initial_smart_now_eoa{!account_deleted && current->incarnation == 0 && initial && initial->incarnation};
    if (initial_smart_now_deleted || initial_smart_now_eoa) {
        if (incarnations_.insert_or_assign(address, initial->incarnation).second) {
            batch_state_size_ += kAddressLength + kIncarnationLength;
        }
    }
}

void Buffer::update_account_code(const evmc::address& address, uint64_t incarnation, const evmc::bytes32& code_hash,
                                 ByteView code) {
    if (code_hash != kEmptyHash && code.empty()) {
        throw std::logic_error{"db::Buffer::update_account_code empty code for non-empty code hash"};
    }

    // Don't overwrite existing code so that views of it that were previously returned by read_code are still valid
    const auto [inserted_or_existing_it, inserted] = hash_to_code_.try_emplace(code_hash, code);
    if (inserted) {
        batch_state_size_ += kHashLength + code.size();
    } else {
        batch_state_size_ += code.size() - inserted_or_existing_it->second.size();
    }

    if (is_traced_bad_gas_address(address) || is_traced_bad_gas_code_hash(code_hash)) {
        SILK_ERROR_M("PsiTriStateTrace",
                     {"write", "update-account-code",
                      "address", to_hex(address.bytes, true),
                      "incarnation", std::to_string(incarnation),
                      "code_hash", to_hex(code_hash.bytes, true),
                      "size", std::to_string(code.size()),
                      "inserted", inserted ? "1" : "0",
                      "cached_size", std::to_string(inserted_or_existing_it->second.size())});
    }

    if (storage_prefix_to_code_hash_.insert_or_assign(storage_prefix(address, incarnation), code_hash).second) {
        batch_state_size_ += kPlainStoragePrefixLength + kHashLength;
    }
}

void Buffer::update_storage(const evmc::address& address, uint64_t incarnation, const evmc::bytes32& location,
                            const evmc::bytes32& initial, const evmc::bytes32& current) {
    if (current == initial) {
        return;
    }
    if (block_num_ >= prune_history_threshold_) {
        changed_storage_.insert(address);
        Bytes initial_val{zeroless_view(initial.bytes)};
        auto& changed_locations{block_storage_changes_[block_num_][address][incarnation]};
        const size_t estimated_size{sizeof(BlockNum) + kAddressLength + kIncarnationLength + kLocationLength + initial_val.size()};
        if (auto it{changed_locations.find(location)}; it != changed_locations.end()) {
            replace_estimated_size(batch_history_size_,
                                   sizeof(BlockNum) + kAddressLength + kIncarnationLength + kLocationLength + it->second.size(),
                                   estimated_size);
            it->second = std::move(initial_val);
        } else {
            batch_history_size_ += estimated_size;
            changed_locations.emplace(location, std::move(initial_val));
        }
    }

    // Iterator in insert_or_assign return value "is pointing at the element that was inserted or updated"
    // so we cannot use it to determine the old value size: we need to use initial instead
    const auto [_, inserted] = storage_[address][incarnation].insert_or_assign(location, current);
    ByteView current_val{zeroless_view(current.bytes)};
    if (inserted) {
        batch_state_size_ += kPlainStoragePrefixLength + kHashLength + current_val.size();
    } else {
        batch_state_size_ += current_val.size() - zeroless_view(initial.bytes).size();
    }
}

void Buffer::write_history_to_db(bool write_change_sets) {
    size_t written_size{0};
    size_t total_written_size{0};

    bool should_trace{log::test_verbosity(log::Level::kTrace)};
    StopWatch sw;
    sw.start();

    if (!block_account_changes_.empty() && write_change_sets) {
        auto account_change_table{open_cursor(txn_, table::kAccountChangeSet)};
        Bytes change_key(sizeof(BlockNum), '\0');
        Bytes change_value(kAddressLength + 128 /* see comment*/,
                           '\0');  // Max size of encoded value is 85. We allocate - once - some byte more for safety
                                   // and avoid reallocation or resizing in the loop
        for (const auto& [block_num, account_changes] : block_account_changes_) {
            endian::store_big_u64(change_key.data(), block_num);
            written_size += sizeof(BlockNum);
            for (const auto& [address, account_encoded] : account_changes) {
                std::memcpy(&change_value[0], address.bytes, kAddressLength);
                std::memcpy(&change_value[kAddressLength], account_encoded.data(), account_encoded.size());
                mdbx::slice k{to_slice(change_key)};
                mdbx::slice v{change_value.data(), kAddressLength + account_encoded.size()};
                mdbx::error::success_or_throw(account_change_table.put(k, &v, MDBX_APPENDDUP));
                written_size += kAddressLength + account_encoded.size();
            }
        }
        total_written_size += written_size;
        if (should_trace) [[unlikely]] {
            auto [_, duration]{sw.lap()};
            log::Trace("Append Account Changes", {"size", human_size(written_size), "in", StopWatch::format(duration)});
        }
        written_size = 0;
    }
    block_account_changes_.clear();

    if (!block_storage_changes_.empty() && write_change_sets) {
        Bytes change_key(sizeof(BlockNum) + kPlainStoragePrefixLength, '\0');
        Bytes change_value(kHashLength + 128, '\0');  // Se comment above (account changes) for explanation about 128

        auto storage_change_table{open_cursor(txn_, table::kStorageChangeSet)};
        for (const auto& [block_num, storage_changes] : block_storage_changes_) {
            endian::store_big_u64(&change_key[0], block_num);
            written_size += sizeof(BlockNum);
            for (const auto& [address, incarnations_locations_values] : storage_changes) {
                std::memcpy(&change_key[sizeof(BlockNum)], address.bytes, kAddressLength);
                written_size += kAddressLength;
                for (const auto& [incarnation, locations_values] : incarnations_locations_values) {
                    endian::store_big_u64(&change_key[sizeof(BlockNum) + kAddressLength], incarnation);
                    written_size += kIncarnationLength;
                    for (const auto& [location, value] : locations_values) {
                        std::memcpy(&change_value[0], location.bytes, kHashLength);
                        std::memcpy(&change_value[kHashLength], value.data(), value.size());
                        mdbx::slice change_value_slice{change_value.data(), kHashLength + value.size()};
                        mdbx::error::success_or_throw(
                            storage_change_table.put(to_slice(change_key), &change_value_slice, MDBX_APPENDDUP));
                        written_size += kLocationLength + value.size();
                    }
                }
            }
        }
        total_written_size += written_size;
        if (should_trace) [[unlikely]] {
            auto [_, duration]{sw.lap()};
            log::Trace("Append Storage Changes", {"size", human_size(written_size), "in", StopWatch::format(duration)});
        }
        written_size = 0;
    }
    block_storage_changes_.clear();

    if (!receipts_.empty()) {
        auto receipt_table{open_cursor(txn_, table::kBlockReceipts)};
        for (const auto& [block_key, receipts] : receipts_) {
            auto k{to_slice(block_key)};
            auto v{to_slice(receipts)};
            mdbx::error::success_or_throw(receipt_table.put(k, &v, MDBX_APPEND));
            written_size += k.length() + v.length();
        }
        receipts_.clear();
        total_written_size += written_size;
        if (should_trace) [[unlikely]] {
            auto [_, duration]{sw.lap()};
            log::Trace("Append Receipts", {"size", human_size(written_size), "in", StopWatch::format(duration)});
        }
        written_size = 0;
    }

    if (!logs_.empty()) {
        auto log_table{open_cursor(txn_, table::kLogs)};
        for (const auto& [log_key, value] : logs_) {
            auto k{to_slice(log_key)};
            auto v{to_slice(value)};
            mdbx::error::success_or_throw(log_table.put(k, &v, MDBX_APPEND));
            written_size += k.length() + v.length();
        }
        logs_.clear();
        total_written_size += written_size;
        if (should_trace) [[unlikely]] {
            auto [_, duration]{sw.lap()};
            log::Trace("Append Logs", {"size", human_size(written_size), "in", StopWatch::format(duration)});
        }
        written_size = 0;
    }

    if (!call_traces_.empty()) {
        Bytes call_traces_key(sizeof(BlockNum), '\0');
        auto call_traces_cursor{txn_.rw_cursor_dup_sort(table::kCallTraceSet)};
        for (const auto& [block_num, account_and_flags_set] : call_traces_) {
            endian::store_big_u64(call_traces_key.data(), block_num);
            written_size += sizeof(BlockNum);
            for (const auto& account_and_flags : account_and_flags_set) {
                auto account_and_flags_slice{to_slice(account_and_flags)};
                mdbx::error::success_or_throw(
                    call_traces_cursor->put(to_slice(call_traces_key), &account_and_flags_slice, MDBX_APPENDDUP));
                written_size += account_and_flags_slice.size();
            }
        }
        call_traces_.clear();
        total_written_size += written_size;
        if (should_trace) [[unlikely]] {
            auto [_, duration]{sw.lap()};
            log::Trace("Append Call Traces", {"size", human_size(written_size), "in", StopWatch::format(duration)});
        }
    }

    auto [finish_time, _]{sw.stop()};
    if (should_trace) [[unlikely]] {
        log::Trace("Flushed history",
                   {"size", human_size(total_written_size), "in", StopWatch::format(sw.since_start(finish_time))});
    }
    batch_txn_write_size_ += total_written_size;
    batch_history_size_ = 0;
}

void Buffer::write_state_to_db() {
    /*
     * ENSURE PlainState updates are Last !!!
     * Also ensure to clear unneeded memory data ASAP to let the OS cache
     * to store more database pages for longer
     */
    reset_cached_cursors();

    size_t written_size{0};
    size_t total_written_size{0};

    bool should_trace{log::test_verbosity(log::Level::kTrace)};
    StopWatch sw;
    sw.start();

    if (!incarnations_.empty()) {
        auto incarnation_table{open_cursor(txn_, table::kIncarnationMap)};
        Bytes data(kIncarnationLength, '\0');
        for (const auto& [address, incarnation] : incarnations_) {
            endian::store_big_u64(&data[0], incarnation);
            incarnation_table.upsert(to_slice(address), to_slice(data));
            written_size += kAddressLength + kIncarnationLength;
        }
        incarnations_.clear();
        total_written_size += written_size;
        if (should_trace) [[unlikely]] {
            auto [_, duration]{sw.lap()};
            log::Trace("Incarnations updated", {"size", human_size(written_size), "in", StopWatch::format(duration)});
        }
        written_size = 0;
    }

    if (!hash_to_code_.empty()) {
        auto code_table{open_cursor(txn_, table::kCode)};
        for (const auto& entry : hash_to_code_) {
            if (is_traced_bad_gas_code_hash(entry.first)) {
                SILK_ERROR_M("PsiTriStateTrace",
                             {"write", "flush-code",
                              "code_hash", to_hex(entry.first.bytes, true),
                              "size", std::to_string(entry.second.size())});
            }
            code_table.upsert(to_slice(entry.first), to_slice(entry.second));
            written_size += kHashLength + entry.second.size();
        }
        hash_to_code_.clear();
        total_written_size += written_size;
        if (should_trace) [[unlikely]] {
            auto [_, duration]{sw.lap()};
            log::Trace("Code updated", {"size", human_size(written_size), "in", StopWatch::format(duration)});
        }
        written_size = 0;
    }

    if (!storage_prefix_to_code_hash_.empty()) {
        auto code_hash_table{open_cursor(txn_, table::kPlainCodeHash)};
        for (const auto& entry : storage_prefix_to_code_hash_) {
            if (is_traced_bad_gas_code_hash(entry.second)) {
                SILK_ERROR_M("PsiTriStateTrace",
                             {"write", "flush-plain-code-hash",
                              "key", to_hex(entry.first, true),
                              "code_hash", to_hex(entry.second.bytes, true)});
            }
            code_hash_table.upsert(to_slice(entry.first), to_slice(entry.second));
            written_size += kAddressLength + kIncarnationLength + kHashLength;
        }
        storage_prefix_to_code_hash_.clear();
        total_written_size += written_size;
        if (should_trace) [[unlikely]] {
            auto [_, duration]{sw.lap()};
            log::Trace("Code Hashes updated", {"size", human_size(written_size), "in", StopWatch::format(duration)});
        }
        written_size = 0;
    }

    // Extract sorted index of unique addresses before inserting into the DB
    absl::btree_set<evmc::address> addresses;
    for (auto& x : accounts_) {
        addresses.insert(x.first);
    }
    for (auto& x : storage_) {
        addresses.insert(x.first);
    }

    if (should_trace) [[unlikely]] {
        auto [_, duration]{sw.lap()};
        log::Trace("Sorted addresses", {"in", StopWatch::format(duration)});
    }

    const bool optimized_plain_state{table::use_psitri_optimized_plain_state()};
    std::unique_ptr<datastore::kvdb::RWCursor> state_table;
    if (optimized_plain_state) {
        state_table = txn_.rw_cursor(table::plain_state_config());
    } else {
        state_table = txn_.rw_cursor_dup_sort(table::plain_state_config());
    }
    auto* state_table_dup = optimized_plain_state ? nullptr : dynamic_cast<datastore::kvdb::RWCursorDupSort*>(state_table.get());
    if (!optimized_plain_state && !state_table_dup) {
        throw std::logic_error("PlainState cursor does not support multivalue operations");
    }
    for (const auto& address : addresses) {
        if (auto it{accounts_.find(address)}; it != accounts_.end()) {
            auto key{to_slice(address)};
            if (optimized_plain_state) {
                state_table->erase(key);
            } else {
                state_table_dup->erase(key, /*whole_multivalue=*/true);  // PlainState is multivalue
            }
            if (it->second.has_value()) {
                Bytes encoded = state::AccountCodec::encode_for_storage(*it->second);
                state_table->upsert(key, to_slice(encoded));
                written_size += kAddressLength + encoded.size();
            }
            accounts_.erase(it);
        }

        if (auto it{storage_.find(address)}; it != storage_.end()) {
            for (const auto& [incarnation, contract_storage] : it->second) {
                Bytes prefix;
                if (!optimized_plain_state) {
                    prefix = storage_prefix(address, incarnation);
                }
                // Extract sorted set of storage locations to insert ordered data into the DB
                absl::btree_set<evmc::bytes32> storage_locations;
                for (auto& storage_entry : contract_storage) {
                    storage_locations.insert(storage_entry.first);
                }
                for (const auto& location : storage_locations) {
                    if (auto storage_it{contract_storage.find(location)}; storage_it != contract_storage.end()) {
                        const auto& value{storage_it->second};
                        if (optimized_plain_state) {
                            upsert_flat_storage_value(*state_table, address, incarnation, location.bytes, value.bytes);
                        } else {
                            upsert_storage_value(*state_table_dup, prefix, location.bytes, value.bytes);
                        }
                        written_size += kPlainStoragePrefixLength + kLocationLength + zeroless_view(value.bytes).size();
                    }
                }
            }
            storage_.erase(it);
        }
    }
    total_written_size += written_size;
    if (should_trace) [[unlikely]] {
        auto [_, duration]{sw.lap()};
        log::Trace("Updated accounts and storage",
                   {"size", human_size(written_size), "in", StopWatch::format(duration)});
    }
    batch_state_size_ = 0;
    batch_txn_write_size_ = 0;

    auto [time_point, _]{sw.stop()};
    log::Info("Flushed state",
              {"size", human_size(total_written_size), "in", StopWatch::format(sw.since_start(time_point))});
}

void Buffer::write_to_db(bool write_change_sets) {
    write_history_to_db(write_change_sets);

    // This should be very last to be written so updated pages
    // have higher chances not to be evicted from RAM
    write_state_to_db();
}

// Erigon WriteReceipts in core/rawdb/accessors_chain.go
void Buffer::insert_receipts(uint64_t block_num, const std::vector<Receipt>& receipts) {
    for (uint32_t i{0}; i < receipts.size(); ++i) {
        if (receipts[i].logs.empty()) {
            continue;
        }

        Bytes key{log_key(block_num, i)};
        Bytes value{cbor_encode(receipts[i].logs)};

        const size_t estimated_size{key.size() + value.size()};
        if (auto it{logs_.find(key)}; it != logs_.end()) {
            replace_estimated_size(batch_history_size_, it->first.size() + it->second.size(), estimated_size);
            it->second = std::move(value);
        } else {
            batch_history_size_ += estimated_size;
            logs_.emplace(std::move(key), std::move(value));
        }
    }

    Bytes key{block_key(block_num)};
    Bytes value{cbor_encode(receipts)};
    const size_t estimated_size{key.size() + value.size()};
    if (auto it{receipts_.find(key)}; it != receipts_.end()) {
        replace_estimated_size(batch_history_size_, it->first.size() + it->second.size(), estimated_size);
        it->second = std::move(value);
    } else {
        batch_history_size_ += estimated_size;
        receipts_.emplace(std::move(key), std::move(value));
    }
}

void Buffer::insert_call_traces(BlockNum block_num, const CallTraces& traces) {
    // Collect and sort all unique accounts touched by the call trace (no duplicates)
    absl::btree_set<evmc::address> touched_accounts;
    for (const auto& sender : traces.senders) {
        touched_accounts.insert(sender);
    }
    for (const auto& recipient : traces.recipients) {
        touched_accounts.insert(recipient);
    }

    if (!touched_accounts.empty()) {
        absl::btree_set<Bytes> values;
        for (const auto& account : touched_accounts) {
            Bytes value(kAddressLength + 1, '\0');
            std::memcpy(value.data(), account.bytes, kAddressLength);
            if (traces.senders.contains(account)) {
                value[kAddressLength] |= 1;
            }
            if (traces.recipients.contains(account)) {
                value[kAddressLength] |= 2;
            }
            values.insert(std::move(value));
        }
        size_t estimated_size{0};
        for (const auto& value : values) {
            estimated_size += sizeof(BlockNum) + value.size();
        }
        if (auto [_, inserted]{call_traces_.emplace(block_num, std::move(values))}; inserted) {
            batch_history_size_ += estimated_size;
        }
    }
}

evmc::bytes32 Buffer::state_root_hash() const {
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

uint64_t Buffer::current_canonical_block() const {
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

std::optional<evmc::bytes32> Buffer::canonical_hash(uint64_t) const {
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

void Buffer::canonize_block(uint64_t, const evmc::bytes32&) {
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

void Buffer::decanonize_block(uint64_t) {
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

void Buffer::insert_block(const Block& block, const evmc::bytes32& hash) {
    uint64_t block_num{block.header.number};
    Bytes key{block_key(block_num, hash.bytes)};
    headers_[key] = block.header;
    bodies_[key] = block.copy_body();

    if (block_num == 0) {
        difficulty_[key] = 0;
    } else {
        std::optional<intx::uint256> parent_difficulty{total_difficulty(block_num - 1, block.header.parent_hash)};
        difficulty_[key] = parent_difficulty.value_or(0);
    }
    difficulty_[key] += block.header.difficulty;
}

std::optional<intx::uint256> Buffer::total_difficulty(uint64_t block_num,
                                                      const evmc::bytes32& block_hash) const noexcept {
    Bytes key{block_key(block_num, block_hash.bytes)};
    if (auto it{difficulty_.find(key)}; it != difficulty_.end()) {
        return it->second;
    }
    return db::read_total_difficulty(txn_, key);
}

std::optional<BlockHeader> Buffer::read_header(uint64_t block_num, const evmc::bytes32& block_hash) const noexcept {
    Bytes key{block_key(block_num, block_hash.bytes)};
    if (auto it{headers_.find(key)}; it != headers_.end()) {
        return it->second;
    }
    return data_model_->read_header(block_num, Hash{block_hash.bytes});
}

bool Buffer::read_body(uint64_t block_num, const evmc::bytes32& block_hash, BlockBody& out) const noexcept {
    Bytes key{block_key(block_num, block_hash.bytes)};
    if (auto it{bodies_.find(key)}; it != bodies_.end()) {
        out = it->second;
        return true;
    }
    return data_model_->read_body(block_num, block_hash.bytes, /*read_senders=*/false, out);
}

std::optional<Account> Buffer::read_account(const evmc::address& address) const noexcept {
    if (auto it{accounts_.find(address)}; it != accounts_.end()) {
        if (is_traced_bad_gas_address(address)) {
            SILK_ERROR_M("PsiTriStateTrace",
                         {"read", "account-cache",
                          "address", to_hex(address.bytes, true),
                          "present", it->second ? "1" : "0",
                          "code_hash", it->second ? to_hex(it->second->code_hash.bytes, true) : ""});
        }
        return it->second;
    }
    if (!historical_block_.has_value()) {
        auto& state_cursor{plain_state_cursor()};
        auto data{state_cursor.find(to_slice(address), /*throw_notfound=*/false)};
        if (!data.done || data.value.empty()) {
            if (is_traced_bad_gas_address(address)) {
                SILK_ERROR_M("PsiTriStateTrace",
                             {"read", "account-db",
                              "address", to_hex(address.bytes, true),
                              "present", "0"});
            }
            return std::nullopt;
        }

        auto acc_res{state::AccountCodec::from_encoded_storage(datastore::kvdb::from_slice(data.value))};
        silkworm::success_or_throw(acc_res);
        Account acc{*acc_res};
        if (is_traced_bad_gas_address(address)) {
            SILK_ERROR_M("PsiTriStateTrace",
                         {"read", "account-db",
                          "address", to_hex(address.bytes, true),
                          "present", "1",
                          "raw", to_hex(datastore::kvdb::from_slice(data.value), true),
                          "nonce", std::to_string(acc.nonce),
                          "incarnation", std::to_string(acc.incarnation),
                          "code_hash", to_hex(acc.code_hash.bytes, true)});
        }

        if (acc.incarnation > 0 && acc.code_hash == kEmptyHash) {
            auto& code_cursor{plain_code_hash_cursor()};
            auto key{storage_prefix(address, acc.incarnation)};
            if (auto code_data = code_cursor.find(to_slice(key), /*throw_notfound=*/false);
                code_data.done && code_data.value.length() == kHashLength) {
                std::memcpy(acc.code_hash.bytes, code_data.value.data(), kHashLength);
                if (is_traced_bad_gas_address(address)) {
                    SILK_ERROR_M("PsiTriStateTrace",
                                 {"read", "plain-code-hash",
                                  "address", to_hex(address.bytes, true),
                                  "incarnation", std::to_string(acc.incarnation),
                                  "code_hash", to_hex(acc.code_hash.bytes, true)});
                }
            } else if (is_traced_bad_gas_address(address)) {
                SILK_ERROR_M("PsiTriStateTrace",
                             {"read", "plain-code-hash",
                              "address", to_hex(address.bytes, true),
                              "incarnation", std::to_string(acc.incarnation),
                              "present", "0"});
            }
        }

        return acc;
    }

    auto db_account{db::read_account(txn_, address, historical_block_)};
    return db_account;
}

ByteView Buffer::read_code(const evmc::address& address, const evmc::bytes32& code_hash) const noexcept {
    if (auto it{hash_to_code_.find(code_hash)}; it != hash_to_code_.end()) {
        if (is_traced_bad_gas_address(address)) {
            SILK_ERROR_M("PsiTriStateTrace",
                         {"read", "code-cache-new",
                          "address", to_hex(address.bytes, true),
                          "code_hash", to_hex(code_hash.bytes, true),
                          "size", std::to_string(it->second.size())});
        }
        return it->second;
    }
    if (auto it{existing_code_.find(code_hash)}; it != existing_code_.end()) {
        if (is_traced_bad_gas_address(address)) {
            SILK_ERROR_M("PsiTriStateTrace",
                         {"read", "code-cache-existing",
                          "address", to_hex(address.bytes, true),
                          "code_hash", to_hex(code_hash.bytes, true),
                          "size", std::to_string(it->second.size())});
        }
        return it->second;
    }
    std::optional<Bytes> code{db::read_code(txn_, code_hash)};
    if (!code) {
        if (is_traced_bad_gas_address(address)) {
            SILK_ERROR_M("PsiTriStateTrace",
                         {"read", "code-db",
                          "address", to_hex(address.bytes, true),
                          "code_hash", to_hex(code_hash.bytes, true),
                          "present", "0"});
        }
        return {};
    }
    auto [it, _]{existing_code_.emplace(code_hash, std::move(*code))};
    if (is_traced_bad_gas_address(address)) {
        SILK_ERROR_M("PsiTriStateTrace",
                     {"read", "code-db",
                      "address", to_hex(address.bytes, true),
                      "code_hash", to_hex(code_hash.bytes, true),
                      "present", "1",
                      "size", std::to_string(it->second.size())});
    }
    return it->second;
}

evmc::bytes32 Buffer::read_storage(const evmc::address& address, uint64_t incarnation,
                                   const evmc::bytes32& location) const noexcept {
    if (auto it1{storage_.find(address)}; it1 != storage_.end()) {
        if (auto it2{it1->second.find(incarnation)}; it2 != it1->second.end()) {
            if (auto it3{it2->second.find(location)}; it3 != it2->second.end()) {
                return it3->second;
            }
        }
    }
    if (!historical_block_.has_value()) {
        std::optional<ByteView> value{};
        auto& state_cursor{plain_state_cursor()};
        if (table::use_psitri_optimized_plain_state()) {
            value = find_flat_storage_value(state_cursor, address, incarnation, location.bytes);
        } else if (auto* dup_cursor = dynamic_cast<datastore::kvdb::ROCursorDupSort*>(&state_cursor)) {
            auto key{storage_prefix(address, incarnation)};
            value = find_value_suffix(*dup_cursor, key, location.bytes);
        }
        if (!value) {
            return {};
        }

        evmc::bytes32 res{};
        SILKWORM_ASSERT(value->size() <= kHashLength);
        std::memcpy(res.bytes + kHashLength - value->size(), value->data(), value->size());
        return res;
    }

    auto db_storage{db::read_storage(txn_, address, incarnation, location, historical_block_)};
    return db_storage;
}

uint64_t Buffer::previous_incarnation(const evmc::address& address) const noexcept {
    if (auto it{incarnations_.find(address)}; it != incarnations_.end()) {
        return it->second;
    }
    std::optional<uint64_t> incarnation{db::read_previous_incarnation(txn_, address, historical_block_)};
    return incarnation.value_or(0);
}

void Buffer::unwind_state_changes(uint64_t) {
    throw std::runtime_error(std::string(__FUNCTION__).append(" not yet implemented"));
}

}  // namespace silkworm::db
