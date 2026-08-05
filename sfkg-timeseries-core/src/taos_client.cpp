#include "sfkg/timeseries/core/internal/taos_client.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <unordered_map>
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
    std::array<TAOS_STMT2_BIND, 2> tags{};
    std::array<TAOS_STMT2_BIND, 5> columns{};

    explicit BoundGroup(const Group& group) : sequence_id(group.sequence_id) {
        table_name = tableName(sequence_id);
        timestamps.reserve(group.points.size());
        doubles.reserve(group.points.size());
        integers.reserve(group.points.size());
        booleans.reserve(group.points.size());
        timestamp_lengths.resize(group.points.size(), sizeof(std::int64_t));
        double_lengths.resize(group.points.size(), sizeof(double));
        integer_lengths.resize(group.points.size(), sizeof(std::int64_t));
        boolean_lengths.resize(group.points.size(), sizeof(std::int8_t));
        string_lengths.resize(group.points.size());
        timestamp_nulls.resize(group.points.size(), 1);
        double_nulls.resize(group.points.size(), 1);
        integer_nulls.resize(group.points.size(), 1);
        boolean_nulls.resize(group.points.size(), 1);
        string_nulls.resize(group.points.size(), 1);

        for (std::size_t index = 0; index < group.points.size(); ++index) {
            const auto& point = *group.points[index];
            timestamps.push_back(point.time);
            timestamp_nulls[index] = 0;
            doubles.push_back(0.0);
            integers.push_back(0);
            booleans.push_back(0);
            std::visit([&](const auto& value) {
                using Value = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<Value, double>) {
                    value_type = 0;
                    doubles.back() = value;
                    double_nulls[index] = 0;
                } else if constexpr (std::is_same_v<Value, std::int64_t>) {
                    value_type = 1;
                    integers.back() = value;
                    integer_nulls[index] = 0;
                } else if constexpr (std::is_same_v<Value, bool>) {
                    value_type = 2;
                    booleans.back() = value ? 1 : 0;
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
        columns[1] = {TSDB_DATA_TYPE_DOUBLE, doubles.data(), double_lengths.data(), double_nulls.data(), static_cast<int>(doubles.size())};
        columns[2] = {TSDB_DATA_TYPE_BIGINT, integers.data(), integer_lengths.data(), integer_nulls.data(), static_cast<int>(integers.size())};
        columns[3] = {TSDB_DATA_TYPE_BOOL, booleans.data(), boolean_lengths.data(), boolean_nulls.data(), static_cast<int>(booleans.size())};
        columns[4] = {TSDB_DATA_TYPE_NCHAR, string_buffer.data(), string_lengths.data(), string_nulls.data(), static_cast<int>(group.points.size())};
    }
};
#endif

}  // namespace

struct TaosClient::Impl {
    std::string host = envOr("SFKG_TAOS_HOST", "127.0.0.1");
    std::string user = envOr("SFKG_TAOS_USER", "root");
    std::string password = envOr("SFKG_TAOS_PASSWORD", "taosdata");
    std::string database = envOr("SFKG_TAOS_DB", "sfkg_timeseries");
    std::uint16_t port{};
    std::uint32_t keep_days{};
    std::string config_error;
    std::string connection_error;
    // These locks serialize use of the shared connection objects. They do not
    // imply that writes and queries share one connection or one lock.
    mutable std::mutex write_mutex;
    mutable std::mutex query_mutex;
#if SFKG_WITH_TAOS
    TAOS* write_connection{};
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
    if (const char* keep = std::getenv("SFKG_TAOS_KEEP_DAYS")) {
        if (!parseKeepDays(keep, &impl_->keep_days)) {
            impl_->config_error =
                "SFKG_TAOS_KEEP_DAYS must be a positive integer";
            return;
        }
    }
#if SFKG_WITH_TAOS
    static std::once_flag init_flag;
    std::call_once(init_flag, [] {
        taos_options(TSDB_OPTION_CONFIGDIR, SFKG_TAOS_CONFIG_DIR);
        taos_init();
    });
    impl_->write_connection = taos_connect(
        impl_->host.c_str(), impl_->user.c_str(), impl_->password.c_str(),
        nullptr, impl_->port);
    impl_->query_connection = taos_connect(
        impl_->host.c_str(), impl_->user.c_str(), impl_->password.c_str(),
        nullptr, impl_->port);
    if (impl_->write_connection == nullptr || impl_->query_connection == nullptr) {
        impl_->connection_error = taos_errstr(nullptr);
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
    if (impl_->write_connection != nullptr) {
        taos_close(impl_->write_connection);
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
    // Schema setup changes the shared write connection state. The query
    // connection is selected under query_mutex below; keep this lock order
    // documented for future reconnect/lifecycle changes.
    std::lock_guard lock(impl_->write_mutex);
    if (impl_->write_connection == nullptr) {
        return unavailable("TDengine is unreachable: " + impl_->connection_error);
    }
    const std::string database = quoteIdentifier(impl_->database);
    std::string create_db =
        "CREATE DATABASE IF NOT EXISTS " + database + " PRECISION 'ms'";
    if (impl_->keep_days != 0) {
        create_db += " KEEP " + std::to_string(impl_->keep_days);
    }
    ResultGuard result{taos_query(impl_->write_connection, create_db.c_str())};
    if (result.result == nullptr || taos_errno(result.result) != 0) {
        return queryError(result.result, "create database");
    }
    const std::string create_stable =
        "CREATE STABLE IF NOT EXISTS " + database + ".`raw_timeseries_data` "
        "(ts TIMESTAMP, d_value DOUBLE, i_value BIGINT, b_value BOOL, "
        "s_value NCHAR(256)) TAGS (sequence_id NCHAR(128), value_type TINYINT)";
    taos_free_result(result.result);
    result.result = taos_query(impl_->write_connection, create_stable.c_str());
    if (result.result == nullptr || taos_errno(result.result) != 0) {
        return queryError(result.result, "create raw_timeseries_data");
    }
    if (taos_select_db(impl_->write_connection, impl_->database.c_str()) != 0) {
        const std::string message = "select TDengine database for writes: " +
            std::string(taos_errstr(nullptr));
        return looksUnavailable(message) ? unavailable(message) : internalError(message);
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
    std::lock_guard lock(impl_->write_mutex);
    if (impl_->write_connection == nullptr) {
        return unavailable("TDengine is unreachable: " + impl_->connection_error);
    }
    const std::string sql =
        "DROP DATABASE IF EXISTS " + quoteIdentifier(impl_->database);
    ResultGuard result{taos_query(impl_->write_connection, sql.c_str())};
    if (result.result == nullptr || taos_errno(result.result) != 0) {
        return queryError(result.result, "drop test database");
    }
    return ok(0, "test database dropped");
#endif
}

OperationResult TaosClient::insertRaw(const TimeseriesBatch& batch) {
    if (batch.points.empty()) {
        return invalidArgument("raw batch must not be empty");
    }
    if (!impl_->config_error.empty()) {
        return invalidArgument(impl_->config_error);
    }
#if !SFKG_WITH_TAOS
    return internalError("TDengine support was disabled at configure time");
#else
    std::map<SequenceId, Group> groups;
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

    // Grouping and type validation above are already outside this lock. The
    // current first version also keeps statement binding here for simplicity;
    // pure buffer construction can be moved out in a later throughput pass.
    std::lock_guard lock(impl_->write_mutex);
    if (impl_->write_connection == nullptr) {
        return unavailable("TDengine is unreachable: " + impl_->connection_error);
    }
    const std::string sql =
        "INSERT INTO ? USING " + quoteIdentifier(impl_->database) +
        ".`raw_timeseries_data` TAGS(?,?) VALUES (?,?,?,?,?)";
    TAOS_STMT2_OPTION option{0, true, true, nullptr, nullptr};
    std::unique_ptr<TAOS_STMT2, decltype(&taos_stmt2_close)> statement(
        taos_stmt2_init(impl_->write_connection, &option), taos_stmt2_close);
    if (!statement) {
        return unavailable("initialize TDengine stmt2: connection unavailable");
    }
    if (taos_stmt2_prepare(statement.get(), sql.c_str(), 0) != 0) {
        return stmtError(statement.get(), "prepare raw insert");
    }

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
        return internalError("TDengine inserted an unexpected number of rows");
    }
    return ok(batch.points.size(), "raw batch inserted");
#endif
}

OperationResult TaosClient::queryRaw(
    const std::vector<SequenceId>& sequence_ids,
    Timestamp start,
    Timestamp end,
    TimeseriesBatch* out) const {
    if (out == nullptr) {
        return invalidArgument("query output must not be null");
    }
    out->points.clear();
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
        return ok(0, "query returned no rows");
    }
#if !SFKG_WITH_TAOS
    return internalError("TDengine support was disabled at configure time");
#else
    // Serialize access to the shared query connection and its result handle.
    std::lock_guard lock(impl_->query_mutex);
    if (impl_->query_connection == nullptr) {
        return unavailable("TDengine is unreachable: " + impl_->connection_error);
    }
    std::ostringstream sql;
    sql << "SELECT ts,d_value,i_value,b_value,s_value,value_type,sequence_id FROM "
        << quoteIdentifier(impl_->database) << ".`raw_timeseries_data` WHERE sequence_id IN (";
    for (std::size_t index = 0; index < sequence_ids.size(); ++index) {
        if (index != 0) {
            sql << ',';
        }
        sql << escapeLiteral(sequence_ids[index]);
    }
    sql << ") AND ts >= " << start << " AND ts < " << end << " ORDER BY ts";

    ResultGuard result{taos_query(impl_->query_connection, sql.str().c_str())};
    if (result.result == nullptr || taos_errno(result.result) != 0) {
        return queryError(result.result, "query raw data");
    }
    const int field_count = taos_num_fields(result.result);
    if (field_count != 7) {
        return internalError("query raw data returned an unexpected schema");
    }
    for (TAOS_ROW row = taos_fetch_row(result.result); row != nullptr; row = taos_fetch_row(result.result)) {
        const int* lengths = taos_fetch_lengths(result.result);
        if (lengths == nullptr) {
            return internalError("query raw data did not return field lengths");
        }
        if (row[0] == nullptr || row[5] == nullptr) {
            return internalError("query raw data returned a null timestamp or value type");
        }
        const auto type = *static_cast<std::int8_t*>(row[5]);
        RawTimeseriesPoint point;
        point.time = *static_cast<std::int64_t*>(row[0]);
        const int value_column = static_cast<int>(type) + 1;
        // taos_fetch_row() advances through result blocks. The row number
        // passed to taos_is_null() is block-local, so using a global counter
        // breaks as soon as a result contains more than one 1024-row block.
        // For this raw query, TDengine represents a NULL field by nullptr.
        if (type < 0 || type > 3 || row[value_column] == nullptr) {
            return internalError("query raw data returned an invalid typed value");
        }
        if (row[6] == nullptr) {
            return internalError("query raw data returned a null sequence_id");
        }
        point.sequence_id = std::string(
            static_cast<char*>(row[6]), static_cast<std::size_t>(lengths[6]));
        if (type == 0) {
            point.value = *static_cast<double*>(row[1]);
        } else if (type == 1) {
            point.value = *static_cast<std::int64_t*>(row[2]);
        } else if (type == 2) {
            point.value = *static_cast<std::int8_t*>(row[3]) != 0;
        } else {
            point.value = std::string(
                static_cast<char*>(row[4]), static_cast<std::size_t>(lengths[4]));
        }
        out->points.push_back(std::move(point));
    }
    std::map<SequenceId, std::size_t> requested_order;
    for (std::size_t index = 0; index < sequence_ids.size(); ++index) {
        requested_order.emplace(sequence_ids[index], index);
    }
    std::stable_sort(
        out->points.begin(), out->points.end(),
        [&requested_order](const RawTimeseriesPoint& left,
                           const RawTimeseriesPoint& right) {
            if (left.time != right.time) {
                return left.time < right.time;
            }
            return requested_order.at(left.sequence_id) <
                requested_order.at(right.sequence_id);
        });
    return ok(out->points.size(), "raw query completed");
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
    sql << "SELECT sequence_id,COUNT(*),MIN(ts),MAX(ts) FROM "
        << quoteIdentifier(impl_->database) << ".`raw_timeseries_data`";
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
    if (taos_num_fields(result.result) != 4) {
        return internalError(
            "history overview returned an unexpected schema");
    }

    std::size_t row_number = 0;
    for (TAOS_ROW row = taos_fetch_row(result.result);
         row != nullptr;
         row = taos_fetch_row(result.result)) {
        const int* lengths = taos_fetch_lengths(result.result);
        if (lengths == nullptr || lengths[0] <= 0) {
            return internalError(
                "history overview did not return a sequence length");
        }
        if (row[0] == nullptr || row[1] == nullptr ||
            taos_is_null(result.result, static_cast<int>(row_number), 2) ||
            taos_is_null(result.result, static_cast<int>(row_number), 3)) {
            return internalError(
                "history overview returned a null sequence or aggregate");
        }
        const auto raw_count = *static_cast<std::int64_t*>(row[1]);
        if (raw_count < 0 ||
            static_cast<std::uint64_t>(raw_count) >
                std::numeric_limits<std::size_t>::max()) {
            return internalError(
                "history overview returned an invalid point count");
        }

        HistorySeriesSummary series;
        series.sequence_id = std::string(
            static_cast<char*>(row[0]), static_cast<std::size_t>(lengths[0]));
        series.point_count = static_cast<std::size_t>(raw_count);
        series.first_time = *static_cast<std::int64_t*>(row[2]);
        series.last_time = *static_cast<std::int64_t*>(row[3]);

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
        ++row_number;
    }
    if (!query.sequence_ids.empty()) {
        std::map<SequenceId, std::size_t> requested_order;
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
