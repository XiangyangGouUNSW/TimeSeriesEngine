#include "sfkg/timeseries/core/internal/taos_client.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#if SFKG_WITH_TAOS
#include <taos.h>
#ifndef SFKG_TAOS_CONFIG_DIR
#define SFKG_TAOS_CONFIG_DIR "/etc/taos"
#endif
#endif

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core::internal {
namespace {

OperationResult unavailable(std::string message) {
    return makeOperationResult(
        OperationCode::Unavailable, 0, 0, std::move(message));
}

OperationResult internalError(std::string message) {
    return makeOperationResult(
        OperationCode::InternalError, 0, 0, std::move(message));
}

std::string envOr(const char* name, const char* fallback) {
    const char* value = std::getenv(name);
    return value == nullptr || *value == '\0' ? fallback : value;
}

bool parsePort(const std::string& text, std::uint16_t* port) {
    if (text.empty()) {
        return false;
    }
    unsigned long parsed = 0;
    for (const char ch : text) {
        if (!std::isdigit(static_cast<unsigned char>(ch))) {
            return false;
        }
        parsed = parsed * 10U + static_cast<unsigned long>(ch - '0');
        if (parsed > std::numeric_limits<std::uint16_t>::max()) {
            return false;
        }
    }
    if (parsed == 0) {
        return false;
    }
    *port = static_cast<std::uint16_t>(parsed);
    return true;
}

bool parseKeepDays(const std::string& text, std::uint32_t* days) {
    if (text.empty()) {
        return false;
    }
    std::uint64_t parsed = 0;
    for (const char ch : text) {
        if (!std::isdigit(static_cast<unsigned char>(ch))) {
            return false;
        }
        parsed = parsed * 10U + static_cast<unsigned>(ch - '0');
        if (parsed > std::numeric_limits<std::uint32_t>::max()) {
            return false;
        }
    }
    if (parsed == 0) {
        return false;
    }
    *days = static_cast<std::uint32_t>(parsed);
    return true;
}

bool parsePositiveSize(const std::string& text, std::size_t* value) {
    if (text.empty()) {
        return false;
    }
    std::uint64_t parsed = 0;
    for (const char ch : text) {
        if (!std::isdigit(static_cast<unsigned char>(ch))) {
            return false;
        }
        parsed = parsed * 10U + static_cast<unsigned>(ch - '0');
        if (parsed > 64) {
            return false;
        }
    }
    if (parsed == 0) {
        return false;
    }
    *value = static_cast<std::size_t>(parsed);
    return true;
}

bool validDatabaseName(const std::string& database) {
    return !database.empty() && database.size() <= 192 &&
           database.find('`') == std::string::npos &&
           database.find('\0') == std::string::npos;
}

std::string quoteIdentifier(const std::string& value) {
    return "`" + value + "`";
}

std::string escapeLiteral(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size() + 2);
    for (const char ch : value) {
        if (ch == '\\' || ch == '\'') {
            escaped.push_back('\\');
        }
        escaped.push_back(ch);
    }
    return "'" + escaped + "'";
}

bool looksUnavailable(std::string message) {
    std::transform(message.begin(), message.end(), message.begin(), [](char ch) {
        return static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    });
    return message.find("connect") != std::string::npos ||
           message.find("network") != std::string::npos ||
           message.find("timeout") != std::string::npos ||
           message.find("unavailable") != std::string::npos ||
           message.find("not connected") != std::string::npos;
}

#if SFKG_WITH_TAOS
std::string taosError(TAOS_RES* result) {
    const char* message = taos_errstr(result);
    return message == nullptr || *message == '\0' ? "unknown TDengine error" : message;
}

OperationResult queryError(TAOS_RES* result, const char* operation) {
    const std::string message = std::string(operation) + ": " + taosError(result);
    return looksUnavailable(message) ? unavailable(message) : internalError(message);
}

OperationResult stmtError(TAOS_STMT2* statement, const char* operation) {
    const char* error = taos_stmt2_error(statement);
    const std::string message = std::string(operation) + ": " +
        (error == nullptr || *error == '\0' ? "unknown TDengine error" : error);
    return looksUnavailable(message) ? unavailable(message) : internalError(message);
}

struct ResultGuard {
    TAOS_RES* result{};
    ~ResultGuard() {
        if (result != nullptr) {
            taos_free_result(result);
        }
    }
};

struct QueryFieldIndexes {
    int timestamp{-1};
    int value{-1};
    int double_value{-1};
    int integer_value{-1};
    int boolean_value{-1};
    int string_value{-1};
    int value_type{-1};
    int sequence_id{-1};
};

struct OverviewFieldIndexes {
    int sequence_id{-1};
    int point_count{-1};
    int first_time{-1};
    int last_time{-1};
};

std::string lowercase(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](char ch) {
        return static_cast<char>(
            std::tolower(static_cast<unsigned char>(ch)));
    });
    return value;
}

bool resolveQueryFields(
    TAOS_RES* result,
    QueryFieldIndexes* indexes,
    bool typed_query) {
    const int field_count = taos_num_fields(result);
    TAOS_FIELD* fields = taos_fetch_fields(result);
    if (field_count <= 0 || fields == nullptr) {
        return false;
    }

    for (int index = 0; index < field_count; ++index) {
        const std::string name = lowercase(fields[index].name);
        int* target = nullptr;
        if (name == "ts") {
            target = &indexes->timestamp;
        } else if (name == "value") {
            target = &indexes->value;
        } else if (name == "d_value") {
            target = &indexes->double_value;
        } else if (name == "i_value") {
            target = &indexes->integer_value;
        } else if (name == "b_value") {
            target = &indexes->boolean_value;
        } else if (name == "s_value") {
            target = &indexes->string_value;
        } else if (name == "value_type") {
            target = &indexes->value_type;
        } else if (name == "sequence_id") {
            target = &indexes->sequence_id;
        }
        if (target != nullptr) {
            if (*target != -1) {
                return false;
            }
            *target = index;
        }
    }

    if (typed_query) {
        return indexes->timestamp != -1 && indexes->value != -1 &&
            indexes->value_type != -1 && indexes->sequence_id != -1;
    }
    return indexes->timestamp != -1 &&
        indexes->double_value != -1 &&
        indexes->integer_value != -1 &&
        indexes->boolean_value != -1 &&
        indexes->string_value != -1 &&
        indexes->value_type != -1 &&
        indexes->sequence_id != -1;
}

const char* valueColumn(TimeseriesValueKind kind) {
    switch (kind) {
        case TimeseriesValueKind::Double:
            return "d_value";
        case TimeseriesValueKind::Int64:
            return "i_value";
        case TimeseriesValueKind::Bool:
            return "b_value";
        case TimeseriesValueKind::String:
            return "s_value";
        case TimeseriesValueKind::Unknown:
            return nullptr;
    }
    return nullptr;
}

int valueTypeCode(TimeseriesValueKind kind) {
    switch (kind) {
        case TimeseriesValueKind::Double:
            return 0;
        case TimeseriesValueKind::Int64:
            return 1;
        case TimeseriesValueKind::Bool:
            return 2;
        case TimeseriesValueKind::String:
            return 3;
        case TimeseriesValueKind::Unknown:
            return -1;
    }
    return -1;
}

std::string decodeNcharColumn(
    const void* column,
    const int* offsets,
    int row_index) {
    const auto* encoded = static_cast<const char*>(column) +
        offsets[row_index];
    std::uint16_t byte_length{};
    std::memcpy(&byte_length, encoded, sizeof(byte_length));
    return std::string(encoded + sizeof(byte_length), byte_length);
}

bool resolveOverviewFields(
    TAOS_RES* result,
    OverviewFieldIndexes* indexes) {
    const int field_count = taos_num_fields(result);
    TAOS_FIELD* fields = taos_fetch_fields(result);
    if (field_count <= 0 || fields == nullptr) {
        return false;
    }

    for (int index = 0; index < field_count; ++index) {
        const std::string name = lowercase(fields[index].name);
        int* target = nullptr;
        if (name == "sequence_id") {
            target = &indexes->sequence_id;
        } else if (name == "point_count") {
            target = &indexes->point_count;
        } else if (name == "first_time") {
            target = &indexes->first_time;
        } else if (name == "last_time") {
            target = &indexes->last_time;
        }
        if (target != nullptr) {
            if (*target != -1) {
                return false;
            }
            *target = index;
        }
    }

    return indexes->sequence_id != -1 &&
        indexes->point_count != -1 &&
        indexes->first_time != -1 &&
        indexes->last_time != -1;
}

bool queryTimingEnabled() {
    const char* value = std::getenv("SFKG_TAOS_QUERY_TIMING");
    return value != nullptr && std::string_view(value) == "1";
}

std::string tableName(const SequenceId& sequence_id) {
    // FNV-1a is deterministic across processes and platforms.  The prefix
    // keeps the generated identifier a legal, non-user-controlled table name.
    std::uint64_t hash = 1469598103934665603ULL;
    for (const unsigned char ch : sequence_id) {
        hash ^= ch;
        hash *= 1099511628211ULL;
    }
    std::ostringstream stream;
    stream << "st_" << std::hex << hash;
    return stream.str();
}

struct Group {
    SequenceId sequence_id;
    std::vector<const RawTimeseriesPoint*> points;
};

struct BoundGroup {
    std::string table_name;
    std::string sequence_id;
    std::int8_t value_type{};
    std::int8_t tag_value_type{};
    std::int32_t tag_sequence_length{};
    std::vector<std::int64_t> timestamps;
    std::vector<double> doubles;
    std::vector<std::int64_t> integers;
    std::vector<std::int8_t> booleans;
    std::vector<char> string_buffer;
    std::vector<std::int32_t> timestamp_lengths;
    std::vector<std::int32_t> double_lengths;
    std::vector<std::int32_t> integer_lengths;
    std::vector<std::int32_t> boolean_lengths;
    std::vector<std::int32_t> string_lengths;
    std::vector<char> timestamp_nulls;
    std::vector<char> double_nulls;
    std::vector<char> integer_nulls;
    std::vector<char> boolean_nulls;
    std::vector<char> string_nulls;
    // Unused value columns are bound as all-null columns.  One shared
    // fallback buffer is enough for them; allocating a full value and
    // null/length array for every possible type wastes work for homogeneous
    // sequences.
    std::vector<std::uint64_t> null_values;
    std::vector<std::int32_t> null_lengths;
    std::vector<char> all_nulls;
    std::array<TAOS_STMT2_BIND, 2> tags{};
    std::array<TAOS_STMT2_BIND, 5> columns{};

    explicit BoundGroup(const Group& group) : sequence_id(group.sequence_id) {
        table_name = tableName(sequence_id);
        const auto point_count = group.points.size();
        const auto type_index = group.points.front()->value.index();
        timestamps.resize(point_count);
        timestamp_lengths.resize(group.points.size(), sizeof(std::int64_t));
        timestamp_nulls.resize(group.points.size(), 1);
        null_values.resize(point_count);
        null_lengths.resize(point_count);
        all_nulls.resize(point_count, 1);

        switch (type_index) {
            case 0:
                doubles.resize(point_count);
                double_lengths.resize(point_count, sizeof(double));
                double_nulls.resize(point_count, 1);
                break;
            case 1:
                integers.resize(point_count);
                integer_lengths.resize(point_count, sizeof(std::int64_t));
                integer_nulls.resize(point_count, 1);
                break;
            case 2:
                booleans.resize(point_count);
                boolean_lengths.resize(point_count, sizeof(std::int8_t));
                boolean_nulls.resize(point_count, 1);
                break;
            case 3: {
                string_lengths.resize(point_count);
                string_nulls.resize(point_count, 1);
                std::size_t string_bytes = 0;
                for (const auto* point : group.points) {
                    string_bytes += std::get<std::string>(point->value).size();
                }
                string_buffer.reserve(string_bytes);
                break;
            }
            default:
                throw std::invalid_argument("unsupported raw value type");
        }

        for (std::size_t index = 0; index < group.points.size(); ++index) {
            const auto& point = *group.points[index];
            timestamps[index] = point.time;
            timestamp_nulls[index] = 0;
            std::visit([&](const auto& value) {
                using Value = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<Value, double>) {
                    value_type = 0;
                    doubles[index] = value;
                    double_nulls[index] = 0;
                } else if constexpr (std::is_same_v<Value, std::int64_t>) {
                    value_type = 1;
                    integers[index] = value;
                    integer_nulls[index] = 0;
                } else if constexpr (std::is_same_v<Value, bool>) {
                    value_type = 2;
                    booleans[index] = value ? 1 : 0;
                    boolean_nulls[index] = 0;
                } else if constexpr (std::is_same_v<Value, std::string>) {
                    value_type = 3;
                    if (value.size() > 1024) {
                        throw std::invalid_argument("string value exceeds NCHAR(256) UTF-8 capacity");
                    }
                    string_buffer.insert(
                        string_buffer.end(), value.begin(), value.end());
                    string_lengths[index] = static_cast<std::int32_t>(value.size());
                    string_nulls[index] = 0;
                }
            }, point.value);
        }
        tag_sequence_length = static_cast<std::int32_t>(sequence_id.size());
        tag_value_type = value_type;
        tags[0] = {TSDB_DATA_TYPE_NCHAR, sequence_id.data(), &tag_sequence_length, nullptr, 1};
        tags[1] = {TSDB_DATA_TYPE_TINYINT, &tag_value_type, nullptr, nullptr, 1};
        columns[0] = {TSDB_DATA_TYPE_TIMESTAMP, timestamps.data(), timestamp_lengths.data(), timestamp_nulls.data(), static_cast<int>(timestamps.size())};
        columns[1] = {TSDB_DATA_TYPE_DOUBLE,
            type_index == 0 ? static_cast<void*>(doubles.data()) : static_cast<void*>(null_values.data()),
            type_index == 0 ? double_lengths.data() : null_lengths.data(),
            type_index == 0 ? double_nulls.data() : all_nulls.data(),
            static_cast<int>(point_count)};
        columns[2] = {TSDB_DATA_TYPE_BIGINT,
            type_index == 1 ? static_cast<void*>(integers.data()) : static_cast<void*>(null_values.data()),
            type_index == 1 ? integer_lengths.data() : null_lengths.data(),
            type_index == 1 ? integer_nulls.data() : all_nulls.data(),
            static_cast<int>(point_count)};
        columns[3] = {TSDB_DATA_TYPE_BOOL,
            type_index == 2 ? static_cast<void*>(booleans.data()) : static_cast<void*>(null_values.data()),
            type_index == 2 ? boolean_lengths.data() : null_lengths.data(),
            type_index == 2 ? boolean_nulls.data() : all_nulls.data(),
            static_cast<int>(point_count)};
        columns[4] = {TSDB_DATA_TYPE_NCHAR,
            type_index == 3 ? static_cast<void*>(string_buffer.data()) : static_cast<void*>(null_values.data()),
            type_index == 3 ? string_lengths.data() : null_lengths.data(),
            type_index == 3 ? string_nulls.data() : all_nulls.data(),
            static_cast<int>(point_count)};
    }
};
#endif

}  // namespace

struct TaosClient::Impl {
    std::string host = envOr("SFKG_TAOS_HOST", "127.0.0.1");
    std::string user = envOr("SFKG_TAOS_USER", "root");
    std::string password = envOr("SFKG_TAOS_PASSWORD", "taosdata");
    std::string database = envOr("SFKG_TAOS_DB", "sfkg_timeseries");
    std::string raw_stable_name =
        envOr("SFKG_TAOS_RAW_STABLE", "raw_timeseries_data");
    std::uint16_t port{};
    std::uint32_t keep_days{};
    std::size_t write_connection_count{4};
    std::string config_error;
    std::string connection_error;
    // Each write worker selects one connection and only serializes with other
    // users of that same connection. This is a connection-pool policy, not a
    // claim that TDengine writes are globally required to be single-threaded.
    struct WriteConnection {
#if SFKG_WITH_TAOS
        TAOS* connection{};
#endif
        mutable std::mutex mutex;
    };
    std::vector<std::unique_ptr<WriteConnection>> write_connections;
    // Query operations intentionally use a separate connection and lock.
    mutable std::mutex query_mutex;
#if SFKG_WITH_TAOS
    TAOS* query_connection{};
#endif
};

TaosClient::TaosClient() : impl_(std::make_unique<Impl>()) {
    const std::string port_text = envOr("SFKG_TAOS_PORT", "6030");
    if (!parsePort(port_text, &impl_->port)) {
        impl_->config_error = "SFKG_TAOS_PORT must be an integer in [1, 65535]";
        return;
    }
    if (!validDatabaseName(impl_->database)) {
        impl_->config_error = "SFKG_TAOS_DB is empty, too long, or contains `";
        return;
    }
    if (!validDatabaseName(impl_->raw_stable_name)) {
        impl_->config_error =
            "SFKG_TAOS_RAW_STABLE is empty, too long, or contains `";
        return;
    }
    if (const char* keep = std::getenv("SFKG_TAOS_KEEP_DAYS")) {
        if (!parseKeepDays(keep, &impl_->keep_days)) {
            impl_->config_error =
                "SFKG_TAOS_KEEP_DAYS must be a positive integer";
            return;
        }
    }
    if (const char* connections = std::getenv("SFKG_TAOS_WRITE_CONNECTIONS")) {
        if (!parsePositiveSize(connections, &impl_->write_connection_count)) {
            impl_->config_error =
                "SFKG_TAOS_WRITE_CONNECTIONS must be an integer in [1, 64]";
            return;
        }
    }
#if SFKG_WITH_TAOS
    static std::once_flag init_flag;
    std::call_once(init_flag, [] {
        taos_options(TSDB_OPTION_CONFIGDIR, SFKG_TAOS_CONFIG_DIR);
        taos_init();
    });
    impl_->write_connections.reserve(impl_->write_connection_count);
    std::string first_connection_error;
    for (std::size_t index = 0;
         index < impl_->write_connection_count;
         ++index) {
        auto connection = std::make_unique<Impl::WriteConnection>();
        connection->connection = taos_connect(
            impl_->host.c_str(), impl_->user.c_str(), impl_->password.c_str(),
            nullptr, impl_->port);
        if (connection->connection == nullptr && first_connection_error.empty()) {
            first_connection_error = taos_errstr(nullptr);
        }
        impl_->write_connections.push_back(std::move(connection));
    }
    impl_->query_connection = taos_connect(
        impl_->host.c_str(), impl_->user.c_str(), impl_->password.c_str(),
        nullptr, impl_->port);
    const bool missing_write_connection = std::any_of(
        impl_->write_connections.begin(),
        impl_->write_connections.end(),
        [](const auto& connection) {
            return connection == nullptr || connection->connection == nullptr;
        });
    if (missing_write_connection || impl_->query_connection == nullptr) {
        impl_->connection_error = first_connection_error.empty()
            ? taos_errstr(nullptr)
            : first_connection_error;
        if (impl_->connection_error.empty()) {
            impl_->connection_error = "TDengine connection failed";
        }
    }
#else
    impl_->config_error = "SFKG_WITH_TAOS is disabled";
#endif
}

TaosClient::~TaosClient() {
#if SFKG_WITH_TAOS
    for (auto& connection : impl_->write_connections) {
        if (connection != nullptr && connection->connection != nullptr) {
            taos_close(connection->connection);
        }
    }
    if (impl_->query_connection != nullptr) {
        taos_close(impl_->query_connection);
    }
#endif
}

OperationResult TaosClient::ensureSchema() {
    if (!impl_->config_error.empty()) {
        return invalidArgument(impl_->config_error);
    }
#if !SFKG_WITH_TAOS
    return internalError("TDengine support was disabled at configure time");
#else
    if (impl_->write_connections.empty()) {
        return unavailable("TDengine is unreachable: " + impl_->connection_error);
    }
    auto& first = *impl_->write_connections.front();
    std::lock_guard first_lock(first.mutex);
    if (first.connection == nullptr) {
        return unavailable("TDengine is unreachable: " + impl_->connection_error);
    }
    const std::string database = quoteIdentifier(impl_->database);
    std::string create_db =
        "CREATE DATABASE IF NOT EXISTS " + database + " PRECISION 'ms'";
    if (impl_->keep_days != 0) {
        create_db += " KEEP " + std::to_string(impl_->keep_days);
    }
    ResultGuard result{taos_query(first.connection, create_db.c_str())};
    if (result.result == nullptr || taos_errno(result.result) != 0) {
        return queryError(result.result, "create database");
    }
    const std::string stable = quoteIdentifier(impl_->raw_stable_name);
    const std::string create_stable =
        "CREATE STABLE IF NOT EXISTS " + database + "." + stable + " "
        "(ts TIMESTAMP, d_value DOUBLE, i_value BIGINT, b_value BOOL, "
        "s_value NCHAR(256)) TAGS (sequence_id NCHAR(128), value_type TINYINT)";
    taos_free_result(result.result);
    result.result = taos_query(first.connection, create_stable.c_str());
    if (result.result == nullptr || taos_errno(result.result) != 0) {
        return queryError(result.result, "create raw data stable");
    }
    if (taos_select_db(first.connection, impl_->database.c_str()) != 0) {
        const std::string message = "select TDengine database for writes: " +
            std::string(taos_errstr(nullptr));
        return looksUnavailable(message) ? unavailable(message) : internalError(message);
    }
    for (std::size_t index = 1;
         index < impl_->write_connections.size();
         ++index) {
        auto& connection = *impl_->write_connections[index];
        std::lock_guard connection_lock(connection.mutex);
        if (connection.connection == nullptr ||
            taos_select_db(connection.connection, impl_->database.c_str()) != 0) {
            return unavailable(
                "select TDengine database for write connection " +
                std::to_string(index) + ": " +
                std::string(taos_errstr(nullptr)));
        }
    }
    {
        // This is a separate connection, so selecting its database does not
        // serialize ordinary history reads with writes.
        std::lock_guard query_lock(impl_->query_mutex);
        if (impl_->query_connection != nullptr &&
            taos_select_db(impl_->query_connection, impl_->database.c_str()) != 0) {
            const std::string message = "select TDengine database for queries: " +
                std::string(taos_errstr(nullptr));
            return looksUnavailable(message) ? unavailable(message) : internalError(message);
        }
    }
    return ok(0, "TDengine schema is ready");
#endif
}

OperationResult TaosClient::dropDatabaseForTesting() {
    if (!impl_->config_error.empty()) {
        return invalidArgument(impl_->config_error);
    }
#if !SFKG_WITH_TAOS
    return internalError("TDengine support was disabled at configure time");
#else
    if (impl_->write_connections.empty()) {
        return unavailable("TDengine is unreachable: " + impl_->connection_error);
    }
    // This method is intentionally test-only. Close every connection first;
    // dropping a database through one connection while the remaining pooled
    // connections still hold that database can make TDengine report
    // "VGroup is offline" during a high-volume test cleanup.
    for (auto& pooled : impl_->write_connections) {
        if (pooled == nullptr) {
            continue;
        }
        std::lock_guard lock(pooled->mutex);
        if (pooled->connection != nullptr) {
            taos_close(pooled->connection);
            pooled->connection = nullptr;
        }
    }
    {
        std::lock_guard lock(impl_->query_mutex);
        if (impl_->query_connection != nullptr) {
            taos_close(impl_->query_connection);
            impl_->query_connection = nullptr;
        }
    }

    TAOS* drop_connection = taos_connect(
        impl_->host.c_str(), impl_->user.c_str(), impl_->password.c_str(),
        nullptr, impl_->port);
    if (drop_connection == nullptr) {
        return unavailable(
            "TDengine is unreachable while dropping test database: " +
            std::string(taos_errstr(nullptr)));
    }
    const std::string sql =
        "DROP DATABASE IF EXISTS " + quoteIdentifier(impl_->database);
    ResultGuard result{taos_query(drop_connection, sql.c_str())};
    if (result.result == nullptr || taos_errno(result.result) != 0) {
        const auto error = queryError(result.result, "drop test database");
        taos_close(drop_connection);
        return error;
    }
    taos_close(drop_connection);
    return ok(0, "test database dropped");
#endif
}

OperationResult TaosClient::insertRaw(const TimeseriesBatch& batch) {
    if (batch.points.empty()) {
        return invalidArgument("raw batch must not be empty");
    }
#if !SFKG_WITH_TAOS
    return internalError("TDengine support was disabled at configure time");
#else
    if (impl_->write_connections.empty()) {
        return unavailable("TDengine is unreachable: " + impl_->connection_error);
    }
    const auto connection_index = std::hash<SequenceId>{}(
        batch.points.front().sequence_id) % impl_->write_connections.size();
    return insertRawOnConnection(connection_index, batch);
#endif
}

OperationResult TaosClient::insertRawOnConnection(
    std::size_t connection_index,
    const TimeseriesBatch& batch) {
    if (batch.points.empty()) {
        return invalidArgument("raw batch must not be empty");
    }
    if (!impl_->config_error.empty()) {
        return invalidArgument(impl_->config_error);
    }
#if !SFKG_WITH_TAOS
    return internalError("TDengine support was disabled at configure time");
#else
    std::unordered_map<SequenceId, Group> groups;
    for (const auto& point : batch.points) {
        if (point.sequence_id.empty()) {
            return invalidArgument("sequence_id must not be empty");
        }
        groups[point.sequence_id].sequence_id = point.sequence_id;
        groups[point.sequence_id].points.push_back(&point);
    }
    for (const auto& [sequence_id, group] : groups) {
        if (group.points.empty()) {
            continue;
        }
        if (sequence_id.size() > 512) {
            return invalidArgument(
                "sequence_id exceeds NCHAR(128) UTF-8 capacity: " + sequence_id);
        }
        const auto type_index = group.points.front()->value.index();
        if (std::any_of(
                group.points.begin(), group.points.end(),
                [type_index](const auto* point) {
                    return point->value.index() != type_index;
                })) {
            return invalidArgument(
                "all values for one sequence_id must have the same type: " +
                sequence_id);
        }
        if (type_index == 3 && std::any_of(
                group.points.begin(), group.points.end(), [](const auto* point) {
                    return std::get<std::string>(point->value).size() > 1024;
                })) {
            return invalidArgument(
                "string value exceeds NCHAR(256) UTF-8 capacity for sequence_id: " +
                sequence_id);
        }
    }

    // Build the bound buffers before taking the connection lock. This work is
    // local to the request and does not need to serialize with other writes.
    std::vector<std::unique_ptr<BoundGroup>> bound_groups;
    std::vector<char*> table_names;
    std::vector<TAOS_STMT2_BIND*> tags;
    std::vector<TAOS_STMT2_BIND*> columns;
    bound_groups.reserve(groups.size());
    table_names.reserve(groups.size());
    tags.reserve(groups.size());
    columns.reserve(groups.size());
    try {
        for (const auto& [sequence_id, group] : groups) {
            (void)sequence_id;
            bound_groups.push_back(std::make_unique<BoundGroup>(group));
            auto& bound = *bound_groups.back();
            table_names.push_back(bound.table_name.data());
            tags.push_back(bound.tags.data());
            columns.push_back(bound.columns.data());
        }
    } catch (const std::exception& exception) {
        return internalError(std::string("prepare raw values: ") + exception.what());
    }

    if (impl_->write_connections.empty()) {
        return unavailable("TDengine is unreachable: " + impl_->connection_error);
    }
    if (connection_index >= impl_->write_connections.size()) {
        return invalidArgument(
            "write connection index is outside the configured connection pool");
    }
    auto& connection = *impl_->write_connections[connection_index];
    std::lock_guard lock(connection.mutex);
    if (connection.connection == nullptr) {
        return unavailable("TDengine is unreachable: " + impl_->connection_error);
    }
    const std::string sql =
        "INSERT INTO ? USING " + quoteIdentifier(impl_->database) +
        "." + quoteIdentifier(impl_->raw_stable_name) +
        " TAGS(?,?) VALUES (?,?,?,?,?)";
    TAOS_STMT2_OPTION option{0, true, true, nullptr, nullptr};
    std::unique_ptr<TAOS_STMT2, decltype(&taos_stmt2_close)> statement(
        taos_stmt2_init(connection.connection, &option), taos_stmt2_close);
    if (!statement) {
        return unavailable("initialize TDengine stmt2: connection unavailable");
    }
    if (taos_stmt2_prepare(statement.get(), sql.c_str(), 0) != 0) {
        return stmtError(statement.get(), "prepare raw insert");
    }

    TAOS_STMT2_BINDV bindv{
        static_cast<int>(bound_groups.size()), table_names.data(), tags.data(), columns.data()};
    if (taos_stmt2_bind_param(statement.get(), &bindv, -1) != 0) {
        return stmtError(statement.get(), "bind raw insert");
    }
    int affected_rows = 0;
    if (taos_stmt2_exec(statement.get(), &affected_rows) != 0) {
        return stmtError(statement.get(), "execute raw insert");
    }
    if (affected_rows != static_cast<int>(batch.points.size())) {
        std::unordered_map<SequenceId, std::unordered_set<Timestamp>>
            timestamps_by_sequence;
        std::size_t unique_point_count = 0;
        for (const auto& point : batch.points) {
            if (timestamps_by_sequence[point.sequence_id].insert(point.time).
                    second) {
                ++unique_point_count;
            }
        }
        const auto duplicate_point_count =
            batch.points.size() - unique_point_count;
        std::ostringstream message;
        message << "TDengine inserted an unexpected number of rows: "
                << "expected=" << batch.points.size()
                << ", affected=" << affected_rows
                << ", unique_points=" << unique_point_count
                << ", duplicate_points=" << duplicate_point_count
                << ", sequences=" << groups.size()
                << ", write_connection=" << connection_index;
        if (duplicate_point_count != 0) {
            message << "; duplicate (sequence_id,timestamp) keys may have "
                       "been upserted by TDengine";
        } else {
            message << "; no duplicate keys were found inside this batch";
        }
        return internalError(message.str());
    }
    return ok(batch.points.size(), "raw batch inserted");
#endif
}
OperationResult TaosClient::queryRaw(
    const std::vector<SequenceId>& sequence_ids,
    Timestamp start,
    Timestamp end,
    TimeseriesBatch* out,
    std::optional<std::int64_t> granularity,
    const std::unordered_map<SequenceId, TimeseriesValueKind>* value_kinds) const {
    if (out == nullptr) {
        return invalidArgument("query output must not be null");
    }
    if (sequence_ids.empty()) {
        return invalidArgument("sequence_ids must not be empty");
    }
    if (start > end) {
        return invalidArgument("query start must not be after end");
    }
    for (const auto& sequence_id : sequence_ids) {
        if (sequence_id.empty()) {
            return invalidArgument("sequence_id must not be empty");
        }
    }
    if (!impl_->config_error.empty()) {
        return invalidArgument(impl_->config_error);
    }
    if (start == end) {
        out->points.clear();
        return ok(0, "query returned no rows");
    }
    if (granularity && *granularity <= 0) {
        return invalidArgument("query granularity must be positive");
    }
#if !SFKG_WITH_TAOS
    return internalError("TDengine support was disabled at configure time");
#else
    const bool timing_enabled = queryTimingEnabled();
    const auto total_started = std::chrono::steady_clock::now();
    TimeseriesBatch temporary;
    temporary.points.reserve(1024);
    bool time_ordered = true;
    std::optional<Timestamp> previous_time;
    std::chrono::duration<double, std::milli> lock_wait{};
    std::chrono::duration<double, std::milli> sql_time{};
    std::chrono::duration<double, std::milli> fetch_decode_time{};

    std::optional<TimeseriesValueKind> typed_kind;
    if (value_kinds != nullptr) {
        for (const auto& sequence_id : sequence_ids) {
            const auto found = value_kinds->find(sequence_id);
            if (found == value_kinds->end() ||
                found->second == TimeseriesValueKind::Unknown) {
                typed_kind.reset();
                break;
            }
            if (!typed_kind) {
                typed_kind = found->second;
            } else if (*typed_kind != found->second) {
                // Keep mixed-type requests on the generic projection. A
                // single typed SQL projection cannot represent them safely.
                typed_kind.reset();
                break;
            }
        }
    }
    bool typed_fallback = false;
    {
        const auto lock_wait_started = std::chrono::steady_clock::now();
        std::unique_lock lock(impl_->query_mutex);
        lock_wait = std::chrono::steady_clock::now() - lock_wait_started;
        if (impl_->query_connection == nullptr) {
            return unavailable(
                "TDengine is unreachable: " + impl_->connection_error);
        }

        bool use_typed_query = typed_kind.has_value();
        for (;;) {
            std::ostringstream sql;
            const char* typed_column = use_typed_query
                ? valueColumn(*typed_kind)
                : nullptr;
            if (use_typed_query) {
                if (granularity) {
                    sql << "SELECT _wstart AS ts,LAST(" << typed_column
                        << ") AS value,LAST(value_type) AS value_type,"
                           "sequence_id AS sequence_id FROM ";
                } else {
                    sql << "SELECT ts AS ts," << typed_column
                        << " AS value,value_type AS value_type,"
                           "sequence_id AS sequence_id FROM ";
                }
            } else if (granularity) {
                sql << "SELECT _wstart AS ts,LAST(d_value) AS d_value,"
                       "LAST(i_value) AS i_value,LAST(b_value) AS b_value,"
                       "LAST(s_value) AS s_value,LAST(value_type) AS value_type,"
                       "sequence_id AS sequence_id FROM ";
            } else {
                sql << "SELECT ts AS ts,d_value AS d_value,i_value AS i_value,"
                       "b_value AS b_value,s_value AS s_value,"
                       "value_type AS value_type,sequence_id AS sequence_id FROM ";
            }
            sql << quoteIdentifier(impl_->database) << "."
                << quoteIdentifier(impl_->raw_stable_name)
                << " WHERE sequence_id IN (";
            for (std::size_t index = 0; index < sequence_ids.size(); ++index) {
                if (index != 0) {
                    sql << ',';
                }
                sql << escapeLiteral(sequence_ids[index]);
            }
            sql << ") AND ts >= " << start << " AND ts < " << end;
            if (granularity) {
                // `a` is TDengine's millisecond interval unit. LAST preserves
                // a generic value type while reducing one sequence to one
                // point per time bucket.
                sql << " PARTITION BY sequence_id INTERVAL ("
                    << *granularity << "a)";
            } else {
                sql << " ORDER BY ts";
            }

            const auto sql_started = std::chrono::steady_clock::now();
            ResultGuard result{
                taos_query(impl_->query_connection, sql.str().c_str())};
            sql_time += std::chrono::steady_clock::now() - sql_started;
            if (result.result == nullptr || taos_errno(result.result) != 0) {
                return queryError(result.result, "query raw data");
            }
            QueryFieldIndexes fields;
            if (!resolveQueryFields(result.result, &fields, use_typed_query)) {
                return internalError(
                    "query raw data result is missing one or more required fields");
            }

            const auto fetch_started = std::chrono::steady_clock::now();
            bool typed_mismatch = false;
            int row_count = 0;
            TAOS_ROW block = nullptr;
            while ((row_count = taos_fetch_block(result.result, &block)) > 0) {
                auto** columns = reinterpret_cast<void**>(block);
                const int* sequence_offsets = taos_get_column_data_offset(
                    result.result, fields.sequence_id);
                const int* string_offsets = use_typed_query
                    ? nullptr
                    : taos_get_column_data_offset(
                        result.result, fields.string_value);
                const int* typed_string_offsets = use_typed_query &&
                    *typed_kind == TimeseriesValueKind::String
                    ? taos_get_column_data_offset(
                        result.result, fields.value)
                    : nullptr;
                if (sequence_offsets == nullptr ||
                    (!use_typed_query && string_offsets == nullptr) ||
                    (use_typed_query &&
                     *typed_kind == TimeseriesValueKind::String &&
                     typed_string_offsets == nullptr)) {
                    return internalError(
                        "query raw data did not return string offsets");
                }
                const auto required_size = temporary.points.size() +
                    static_cast<std::size_t>(row_count);
                if (temporary.points.capacity() < required_size) {
                    temporary.points.reserve(std::max(
                        required_size, temporary.points.capacity() * 2));
                }
                for (int row_index = 0; row_index < row_count; ++row_index) {
                    auto* timestamps = static_cast<std::int64_t*>(
                        columns[fields.timestamp]);
                    auto* types = static_cast<std::int8_t*>(
                        columns[fields.value_type]);
                    if (timestamps == nullptr || types == nullptr ||
                        taos_is_null(result.result, row_index,
                                     fields.timestamp) ||
                        taos_is_null(result.result, row_index,
                                     fields.value_type)) {
                        return internalError(
                            "query raw data returned a null timestamp or value type");
                    }
                    const auto type = types[row_index];
                    RawTimeseriesPoint point;
                    point.time = timestamps[row_index];
                    if (previous_time && point.time < *previous_time) {
                        time_ordered = false;
                    }
                    previous_time = point.time;

                    int value_column = -1;
                    if (use_typed_query) {
                        if (type != valueTypeCode(*typed_kind) ||
                            taos_is_null(result.result, row_index,
                                         fields.value)) {
                            typed_mismatch = true;
                            break;
                        }
                        value_column = fields.value;
                    } else if (type == 0) {
                        value_column = fields.double_value;
                    } else if (type == 1) {
                        value_column = fields.integer_value;
                    } else if (type == 2) {
                        value_column = fields.boolean_value;
                    } else if (type == 3) {
                        value_column = fields.string_value;
                    }
                    if (type < 0 || type > 3 ||
                        taos_is_null(result.result, row_index, value_column)) {
                        return internalError(
                            "query raw data returned an invalid typed value");
                    }
                    if (taos_is_null(result.result, row_index,
                                     fields.sequence_id)) {
                        return internalError(
                            "query raw data returned a null sequence_id");
                    }
                    point.sequence_id = decodeNcharColumn(
                        columns[fields.sequence_id], sequence_offsets, row_index);
                    if (use_typed_query) {
                        auto* values = columns[fields.value];
                        switch (*typed_kind) {
                            case TimeseriesValueKind::Double:
                                point.value = static_cast<double*>(
                                    values)[row_index];
                                break;
                            case TimeseriesValueKind::Int64:
                                point.value = static_cast<std::int64_t*>(
                                    values)[row_index];
                                break;
                            case TimeseriesValueKind::Bool:
                                point.value = static_cast<std::int8_t*>(
                                    values)[row_index] != 0;
                                break;
                            case TimeseriesValueKind::String:
                                point.value = decodeNcharColumn(
                                    values, typed_string_offsets, row_index);
                                break;
                            case TimeseriesValueKind::Unknown:
                                return internalError(
                                    "typed query has an unknown value kind");
                        }
                    } else if (type == 0) {
                        point.value = static_cast<double*>(
                            columns[fields.double_value])[row_index];
                    } else if (type == 1) {
                        point.value = static_cast<std::int64_t*>(
                            columns[fields.integer_value])[row_index];
                    } else if (type == 2) {
                        point.value = static_cast<std::int8_t*>(
                            columns[fields.boolean_value])[row_index] != 0;
                    } else {
                        point.value = decodeNcharColumn(
                            columns[fields.string_value], string_offsets, row_index);
                    }
                    temporary.points.push_back(std::move(point));
                }
                if (typed_mismatch) {
                    break;
                }
            }
            if (row_count < 0) {
                return queryError(result.result, "fetch raw data block");
            }
            fetch_decode_time += std::chrono::steady_clock::now() - fetch_started;
            if (!typed_mismatch) {
                break;
            }

            // A stale or mixed physical type can exist in historical data if
            // a sequence configuration was changed. Retry with the generic
            // projection rather than silently dropping those rows.
            typed_fallback = true;
            use_typed_query = false;
            temporary.points.clear();
            time_ordered = true;
            previous_time.reset();
        }
        // ResultGuard releases TAOS_RES while query_mutex is still held.
    }

    std::unordered_map<SequenceId, std::size_t> requested_order;
    for (std::size_t index = 0; index < sequence_ids.size(); ++index) {
        requested_order.emplace(sequence_ids[index], index);
    }
    const auto requested_order_less = [&requested_order](
        const RawTimeseriesPoint& left,
        const RawTimeseriesPoint& right) {
        return requested_order.at(left.sequence_id) <
            requested_order.at(right.sequence_id);
    };
    const auto sort_started = std::chrono::steady_clock::now();
    if (!time_ordered) {
        // A grouped/windowed TDengine query may return partitions rather than
        // one globally time-ordered stream. Preserve the API contract in this
        // fallback path.
        std::stable_sort(
            temporary.points.begin(), temporary.points.end(),
            [&requested_order](const RawTimeseriesPoint& left,
                               const RawTimeseriesPoint& right) {
                if (left.time != right.time) {
                    return left.time < right.time;
                }
                return requested_order.at(left.sequence_id) <
                    requested_order.at(right.sequence_id);
            });
    } else {
        // SQL ORDER BY ts already established the primary order. Only sort
        // equal-timestamp runs to enforce requested sequence order, avoiding
        // an O(n log n) sort for the common monotonic case.
        for (std::size_t begin = 0; begin < temporary.points.size();) {
            std::size_t finish = begin + 1;
            while (finish < temporary.points.size() &&
                   temporary.points[finish].time == temporary.points[begin].time) {
                ++finish;
            }
            if (finish - begin > 1) {
                std::stable_sort(
                    temporary.points.begin() +
                        static_cast<std::ptrdiff_t>(begin),
                    temporary.points.begin() +
                        static_cast<std::ptrdiff_t>(finish),
                    requested_order_less);
            }
            begin = finish;
        }
    }
    const std::chrono::duration<double, std::milli> sort_time =
        std::chrono::steady_clock::now() - sort_started;
    const std::chrono::duration<double, std::milli> total_time =
        std::chrono::steady_clock::now() - total_started;
    const auto point_count = temporary.points.size();
    out->points = std::move(temporary.points);
    if (timing_enabled) {
        std::clog << "queryRaw_timing points=" << point_count
                  << " lock_wait_ms=" << lock_wait.count()
                  << " sql_ms=" << sql_time.count()
                  << " fetch_decode_ms=" << fetch_decode_time.count()
                  << " sort_ms=" << sort_time.count()
                  << " total_ms=" << total_time.count()
                  << " time_ordered=" << (time_ordered ? "true" : "false")
                  << " fetch_mode=block"
                  << " typed_projection="
                  << ((typed_kind && !typed_fallback) ? "true" : "false")
                  << " granularity_ms="
                  << (granularity ? std::to_string(*granularity) : "raw")
                  << '\n';
    }
    return ok(point_count, "raw query completed");
#endif
}

OperationResult TaosClient::queryHistoryOverview(
    const HistoryOverviewQuery& query,
    HistoryOverview* out) const {
    if (out == nullptr) {
        return invalidArgument("overview output must not be null");
    }
    *out = HistoryOverview{};
    if (query.start_time && query.end_time &&
        *query.start_time > *query.end_time) {
        return invalidArgument(
            "overview start_time must not be after end_time");
    }
    for (const auto& sequence_id : query.sequence_ids) {
        if (sequence_id.empty()) {
            return invalidArgument("sequence_id must not be empty");
        }
    }
    if (!impl_->config_error.empty()) {
        return invalidArgument(impl_->config_error);
    }
    if (query.start_time && query.end_time &&
        *query.start_time == *query.end_time) {
        return ok(0, "history overview returned no rows");
    }
#if !SFKG_WITH_TAOS
    return internalError("TDengine support was disabled at configure time");
#else
    // Overview queries use the same dedicated query connection as raw reads;
    // they are independent of the write connection lock.
    std::lock_guard lock(impl_->query_mutex);
    if (impl_->query_connection == nullptr) {
        return unavailable("TDengine is unreachable: " + impl_->connection_error);
    }

    std::ostringstream sql;
    sql << "SELECT sequence_id AS sequence_id,"
           "COUNT(*) AS point_count,MIN(ts) AS first_time,"
           "MAX(ts) AS last_time FROM "
        << quoteIdentifier(impl_->database) << "."
        << quoteIdentifier(impl_->raw_stable_name);
    if (!query.sequence_ids.empty() || query.start_time || query.end_time) {
        sql << " WHERE ";
        bool has_condition = false;
        if (!query.sequence_ids.empty()) {
            sql << "sequence_id IN (";
            for (std::size_t index = 0;
                 index < query.sequence_ids.size();
                 ++index) {
                if (index != 0) {
                    sql << ',';
                }
                sql << escapeLiteral(query.sequence_ids[index]);
            }
            sql << ')';
            has_condition = true;
        }
        if (query.start_time) {
            if (has_condition) {
                sql << " AND ";
            }
            sql << "ts >= " << *query.start_time;
            has_condition = true;
        }
        if (query.end_time) {
            if (has_condition) {
                sql << " AND ";
            }
            sql << "ts < " << *query.end_time;
        }
    }
    sql << " GROUP BY sequence_id";

    ResultGuard result{taos_query(impl_->query_connection, sql.str().c_str())};
    if (result.result == nullptr || taos_errno(result.result) != 0) {
        return queryError(result.result, "query history overview");
    }
    OverviewFieldIndexes fields;
    if (!resolveOverviewFields(result.result, &fields)) {
        return internalError(
            "history overview result is missing one or more required fields");
    }

    for (TAOS_ROW row = taos_fetch_row(result.result);
         row != nullptr;
         row = taos_fetch_row(result.result)) {
        const int* lengths = taos_fetch_lengths(result.result);
        if (lengths == nullptr ||
            lengths[fields.sequence_id] <= 0) {
            return internalError(
                "history overview did not return a sequence length");
        }
        if (row[fields.sequence_id] == nullptr ||
            row[fields.point_count] == nullptr ||
            row[fields.first_time] == nullptr ||
            row[fields.last_time] == nullptr) {
            return internalError(
                "history overview returned a null sequence or aggregate");
        }
        const auto raw_count = *static_cast<std::int64_t*>(
            row[fields.point_count]);
        if (raw_count < 0 ||
            static_cast<std::uint64_t>(raw_count) >
                std::numeric_limits<std::size_t>::max()) {
            return internalError(
                "history overview returned an invalid point count");
        }

        HistorySeriesSummary series;
        series.sequence_id = std::string(
            static_cast<char*>(row[fields.sequence_id]),
            static_cast<std::size_t>(lengths[fields.sequence_id]));
        series.point_count = static_cast<std::size_t>(raw_count);
        series.first_time = *static_cast<std::int64_t*>(
            row[fields.first_time]);
        series.last_time = *static_cast<std::int64_t*>(
            row[fields.last_time]);

        if (out->total_point_count >
            std::numeric_limits<std::size_t>::max() - series.point_count) {
            return internalError("history overview point count overflowed");
        }
        out->total_point_count += series.point_count;
        out->series.push_back(series);
        if (!out->first_time || *series.first_time < *out->first_time) {
            out->first_time = series.first_time;
        }
        if (!out->last_time || *series.last_time > *out->last_time) {
            out->last_time = series.last_time;
        }
    }
    if (!query.sequence_ids.empty()) {
        std::unordered_map<SequenceId, std::size_t> requested_order;
        for (std::size_t index = 0; index < query.sequence_ids.size(); ++index) {
            requested_order.emplace(query.sequence_ids[index], index);
        }
        std::stable_sort(
            out->series.begin(), out->series.end(),
            [&requested_order](const HistorySeriesSummary& left,
                               const HistorySeriesSummary& right) {
                return requested_order.at(left.sequence_id) <
                    requested_order.at(right.sequence_id);
            });
    } else {
        std::sort(
            out->series.begin(), out->series.end(),
            [](const HistorySeriesSummary& left,
               const HistorySeriesSummary& right) {
                return left.sequence_id < right.sequence_id;
            });
    }
    out->column_names.reserve(out->series.size());
    for (const auto& series : out->series) {
        out->column_names.push_back(series.sequence_id);
    }
    out->sequence_count = out->series.size();
    return ok(out->total_point_count, "history overview completed");
#endif
}

}  // namespace sfkg::timeseries::core::internal
