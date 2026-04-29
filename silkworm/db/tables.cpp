// Copyright 2025 The Silkworm Authors
// SPDX-License-Identifier: Apache-2.0

#include "tables.hpp"

#include <cstdlib>
#include <stdexcept>
#include <string_view>

#include <silkworm/db/access_layer.hpp>

namespace silkworm::db::table {

namespace {

bool env_enabled(const char* value) {
    if (value == nullptr) {
        return false;
    }
    const std::string_view flag{value};
    return !flag.empty() && flag != "0" && flag != "false" && flag != "FALSE" && flag != "off" && flag != "OFF";
}

}  // namespace

bool use_psitri_optimized_hashed_storage() {
#ifdef USE_PSITRI
    static const bool enabled = [] {
        if (const char* layout = std::getenv("SILKWORM_DB_LAYOUT")) {
            return std::string_view{layout} == "psitri-optimized";
        }
        return env_enabled(std::getenv("SILKWORM_PSITRI_OPTIMIZED_LAYOUT"));
    }();
    return enabled;
#else
    return false;
#endif
}

bool use_psitri_optimized_plain_state() {
    return use_psitri_optimized_hashed_storage();
}

MapConfig plain_state_config() {
    return use_psitri_optimized_plain_state() ? kPlainStatePsitriOptimized : kPlainState;
}

MapConfig hashed_storage_config() {
    return use_psitri_optimized_hashed_storage() ? kHashedStoragePsitriOptimized : kHashedStorage;
}

void check_or_create_chaindata_tables(RWTxn& txn) {
    for (const auto& table_config : kChainDataTables) {
        const auto config = table_config.name == kHashedStorageName
                                ? hashed_storage_config()
                                : table_config.name == kPlainStateName ? plain_state_config() : table_config;

        if (has_map(txn, config.name)) {
            ::mdbx::map_handle table_map = txn->open_map(config.name_str());
            auto table_info{txn->get_handle_info(table_map)};
            auto table_key_mode{table_info.key_mode()};
            auto table_value_mode{table_info.value_mode()};
            if (table_key_mode != config.key_mode || table_value_mode != config.value_mode) {
                throw std::runtime_error("MDBX Table schema incompatible: " + std::string(config.name) +
                                         " has incompatible flags.");
            }
            continue;
        }
        // Create missing table
        (void)txn->create_map(config.name_str(), config.key_mode, config.value_mode);  // Will throw if tx is RO
    }

    auto db_schema_version{db::read_schema_version(txn)};
    if (!db_schema_version.has_value()) {
        db::write_schema_version(txn, kRequiredSchemaVersion);
    } else if (db_schema_version.value() != kRequiredSchemaVersion) {
        throw std::runtime_error("Incompatible schema version. Expected " + kRequiredSchemaVersion.to_string() +
                                 " got " + db_schema_version.value().to_string());
    }
}

std::optional<MapConfig> get_map_config(std::string_view map_name) {
    for (const auto& table_config : kChainDataTables) {
        if (table_config.name == map_name) {
            if (table_config.name == kHashedStorageName) {
                return hashed_storage_config();
            }
            if (table_config.name == kPlainStateName) {
                return plain_state_config();
            }
            return table_config;
        }
    }

    return std::nullopt;
}

}  // namespace silkworm::db::table
