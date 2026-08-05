#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "sfkg/timeseries/core/history_query_service.hpp"
#include "sfkg/timeseries/core/ingest_service.hpp"
#include "sfkg/timeseries/core/internal/taos_client.hpp"
#include "sfkg/timeseries/core/runtime_config_registry.hpp"
#include "sfkg/timeseries/core/storage_service.hpp"
#include "sfkg/timeseries/core/window_service.hpp"

namespace core = sfkg::timeseries::core;

namespace {

const std::vector<std::string> kColumns{
    "HUFL", "HULL", "MUFL", "MULL", "LUFL", "LULL", "OT"};
constexpr const char* kDataSourceId = "ETTh1.csv";

const char* operationName(core::OperationCode code) {
    switch (code) {
    case core::OperationCode::Ok: return "OK";
    case core::OperationCode::PartialSuccess: return "PARTIAL_SUCCESS";
    case core::OperationCode::InvalidArgument: return "INVALID_ARGUMENT";
    case core::OperationCode::NotFound: return "NOT_FOUND";
    case core::OperationCode::FailedPrecondition: return "FAILED_PRECONDITION";
    case core::OperationCode::Unavailable: return "UNAVAILABLE";
    case core::OperationCode::InternalError: return "INTERNAL_ERROR";
    case core::OperationCode::NotImplemented: return "NOT_IMPLEMENTED";
    }
    return "UNKNOWN";
}

void printOperation(const char* name, const core::OperationResult& result) {
    std::cout << name << ": " << operationName(result.code)
              << " success=" << result.success_count
              << " failed=" << result.failed_count
              << " message=" << result.message << '\n';
}

bool successful(const core::OperationResult& result) {
    return result.code == core::OperationCode::Ok ||
           result.code == core::OperationCode::PartialSuccess;
}

std::vector<std::string> splitCsvLine(const std::string& line) {
    std::vector<std::string> fields;
    std::stringstream stream(line);
    std::string field;
    while (std::getline(stream, field, ',')) {
        fields.push_back(field);
    }
    return fields;
}

bool fixedNumber(std::string_view text, std::size_t offset, std::size_t count,
                 int* value) {
    if (offset + count > text.size()) return false;
    int parsed = 0;
    for (std::size_t index = offset; index < offset + count; ++index) {
        if (text[index] < '0' || text[index] > '9') return false;
        parsed = parsed * 10 + (text[index] - '0');
    }
    *value = parsed;
    return true;
}

std::int64_t daysFromCivil(int year, unsigned month, unsigned day) {
    year -= month <= 2;
    const int era = (year >= 0 ? year : year - 399) / 400;
    const unsigned yearOfEra = static_cast<unsigned>(year - era * 400);
    const unsigned shiftedMonth = static_cast<unsigned>(
        static_cast<int>(month) + (month > 2 ? -3 : 9));
    const unsigned dayOfYear = (153 * shiftedMonth + 2) / 5 + day - 1;
    const unsigned dayOfEra = yearOfEra * 365 + yearOfEra / 4 -
        yearOfEra / 100 + dayOfYear;
    return static_cast<std::int64_t>(era) * 146097 +
        static_cast<std::int64_t>(dayOfEra) - 719468;
}

bool parseTimestamp(std::string_view text, core::Timestamp* timestamp) {
    if (text.size() != 19 || text[4] != '-' || text[7] != '-' ||
        text[10] != ' ' || text[13] != ':' || text[16] != ':') return false;
    int year = 0;
    int month = 0;
    int day = 0;
    int hour = 0;
    int minute = 0;
    int second = 0;
    if (!fixedNumber(text, 0, 4, &year) ||
        !fixedNumber(text, 5, 2, &month) ||
        !fixedNumber(text, 8, 2, &day) ||
        !fixedNumber(text, 11, 2, &hour) ||
        !fixedNumber(text, 14, 2, &minute) ||
        !fixedNumber(text, 17, 2, &second)) return false;
    *timestamp = daysFromCivil(year, static_cast<unsigned>(month),
                               static_cast<unsigned>(day)) * 86'400'000LL +
        static_cast<core::Timestamp>(hour) * 3'600'000LL +
        static_cast<core::Timestamp>(minute) * 60'000LL +
        static_cast<core::Timestamp>(second) * 1'000LL;
    return true;
}

bool parseDouble(const std::string& text, double* value) {
    try {
        std::size_t consumed = 0;
        *value = std::stod(text, &consumed);
        return consumed == text.size();
    } catch (const std::exception&) {
        return false;
    }
}

std::string formatTimestamp(core::Timestamp timestamp) {
    const std::time_t seconds = static_cast<std::time_t>(timestamp / 1'000);
    std::tm utc{};
    gmtime_r(&seconds, &utc);
    std::ostringstream output;
    output << std::put_time(&utc, "%Y-%m-%d %H:%M:%S");
    return output.str();
}

void printPoint(const core::RawTimeseriesPoint& point) {
    std::cout << "  " << formatTimestamp(point.time) << ' '
              << point.sequence_id << " = "
              << std::get<double>(point.value) << '\n';
}

class DatabaseCleanup {
public:
    DatabaseCleanup(core::internal::TaosClient& client, bool keep_database)
        : client_(client), keep_database_(keep_database) {}

    ~DatabaseCleanup() {
        if (keep_database_) {
            std::cout << "keeping demo database\n";
            return;
        }
        const auto result = client_.dropDatabaseForTesting();
        printOperation("cleanup demo database", result);
    }

private:
    core::internal::TaosClient& client_;
    bool keep_database_;
};

}  // namespace

int main(int argc, char* argv[]) {
    const std::string path = argc > 1 ? argv[1] : "../ETTh1.csv";
    // ETTh1 contains 2016 timestamps. Use a long retention period by default
    // so a fresh demo database accepts the historical sample times.
    if (std::getenv("SFKG_TAOS_KEEP_DAYS") == nullptr) {
        setenv("SFKG_TAOS_KEEP_DAYS", "20000", 1);
    }
    std::size_t requested_rows = 24;
    if (argc > 2 && std::string(argv[2]) != "all") {
        try {
            requested_rows = static_cast<std::size_t>(std::stoull(argv[2]));
        } catch (const std::exception&) {
            std::cerr << "row count must be a non-negative integer or all\n";
            return 1;
        }
    }
    const bool keep_database = argc > 3 &&
        std::string(argv[3]) == "--keep-db";

    std::ifstream input(path);
    if (!input) {
        std::cerr << "cannot open ETTh1 CSV: " << path << '\n';
        return 1;
    }
    std::string line;
    if (!std::getline(input, line)) {
        std::cerr << "ETTh1 CSV is empty\n";
        return 1;
    }
    const auto header = splitCsvLine(line);
    if (header.size() != kColumns.size() + 1 || header[0] != "date") {
        std::cerr << "unexpected ETTh1 header\n";
        return 1;
    }
    for (std::size_t index = 0; index < kColumns.size(); ++index) {
        if (header[index + 1] != kColumns[index]) {
            std::cerr << "unexpected ETTh1 column: " << header[index + 1]
                      << '\n';
            return 1;
        }
    }

    std::vector<std::string> sequence_ids;
    for (const auto& column : kColumns) sequence_ids.push_back("ETTh1_" + column);

    core::RuntimeConfigRegistry registry;
    core::RuntimeConfigSnapshot<core::RuntimeInstanceConfig> configs;
    for (std::size_t index = 0; index < kColumns.size(); ++index) {
        configs.items.push_back({
            sequence_ids[index], kDataSourceId, kColumns[index], "ETTh1",
            "double"});
    }
    const auto registered = registry.upsertInstanceConfigs(configs);
    printOperation("sync ETTh1 instance configs", registered);
    if (!successful(registered)) return 1;

    std::vector<core::TimeseriesIngestData> input_points;
    core::Timestamp first_time = std::numeric_limits<core::Timestamp>::max();
    core::Timestamp last_time = std::numeric_limits<core::Timestamp>::min();
    std::size_t csv_rows = 0;
    while (std::getline(input, line)) {
        if (line.empty()) continue;
        if (requested_rows != 0 && csv_rows >= requested_rows) break;
        const auto fields = splitCsvLine(line);
        if (fields.size() != kColumns.size() + 1) {
            std::cerr << "invalid field count at CSV row " << csv_rows + 2 << '\n';
            return 1;
        }
        core::Timestamp timestamp = 0;
        if (!parseTimestamp(fields[0], &timestamp)) {
            std::cerr << "invalid timestamp at CSV row " << csv_rows + 2 << '\n';
            return 1;
        }
        first_time = std::min(first_time, timestamp);
        last_time = std::max(last_time, timestamp);
        for (std::size_t index = 0; index < kColumns.size(); ++index) {
            double value = 0.0;
            if (!parseDouble(fields[index + 1], &value)) {
                std::cerr << "invalid value at CSV row " << csv_rows + 2
                          << ", column " << kColumns[index] << '\n';
                return 1;
            }
            input_points.push_back({
                std::nullopt, kDataSourceId, kColumns[index], timestamp, value});
        }
        ++csv_rows;
    }
    if (input_points.empty()) {
        std::cerr << "no ETTh1 points selected\n";
        return 1;
    }

    std::cout << "\nETTh1 direct ingest demo\n"
              << "input: " << path << "\n"
              << "CSV rows: " << csv_rows << "\n"
              << "input points: " << input_points.size() << "\n"
              << "time range: " << formatTimestamp(first_time) << " .. "
              << formatTimestamp(last_time) << "\n"
              << "database: "
              << (std::getenv("SFKG_TAOS_DB") != nullptr
                      ? std::getenv("SFKG_TAOS_DB")
                      : "sfkg_timeseries")
              << "\n\n";

    core::internal::TaosClient taos;
    DatabaseCleanup cleanup(taos, keep_database);
    const auto schema = taos.ensureSchema();
    printOperation("ensure TDengine schema", schema);
    if (!successful(schema)) return 1;

    core::IngestService ingest(registry);
    core::StorageService storage(taos);
    core::WindowService window;
    core::HistoryQueryService history(registry, taos);

    const auto resolved = ingest.ingestAndResolveData(input_points);
    printOperation("resolve ETTh1 ingest data", resolved.operation);
    if (!successful(resolved.operation)) return 1;

    const auto stored = storage.writeRawData(resolved.resolved_data);
    printOperation("write cold data", stored);
    const auto windowed = window.buildTimeWindow(
        resolved.resolved_data, 24 * 60 * 60 * 1'000LL);
    printOperation("update hot window", windowed);
    if (!successful(stored) || !successful(windowed)) return 1;

    const auto overview = history.queryHistoryOverview(
        {sequence_ids, first_time, last_time + 1});
    printOperation("query history overview", overview.operation);
    std::cout << "overview total points: "
              << overview.overview.total_point_count << '\n';
    for (const auto& series : overview.overview.series) {
        std::cout << "  " << series.sequence_id << ": "
                  << series.point_count << " points\n";
    }

    const auto history_result = history.queryHistoryData(
        {sequence_ids, first_time, last_time + 1, std::nullopt});
    printOperation("query history data", history_result.operation);
    std::cout << "history points returned: "
              << history_result.data.points.size() << '\n';
    const auto print_count = std::min<std::size_t>(
        5, history_result.data.points.size());
    std::cout << "first " << print_count << " history points:\n";
    for (std::size_t index = 0; index < print_count; ++index) {
        printPoint(history_result.data.points[index]);
    }

    core::WindowQuery window_query;
    window_query.sequence_ids = sequence_ids;
    const auto hot_result = window.queryWindowData(window_query);
    printOperation("query hot window", hot_result.operation);
    std::cout << "hot points returned: " << hot_result.operation.success_count
              << '\n';

    std::cout << "\nInteractive query menu\n"
              << "  1 - query all ETTh1 history overview\n"
              << "  2 - query history points for one sequence\n"
              << "  3 - query hot window for one sequence\n"
              << "  q - quit\n";
    std::string command;
    while (std::cout << "query> " && std::cin >> command) {
        if (command == "q") {
            break;
        }
        if (command == "1") {
            const auto result = history.queryHistoryOverview(
                {sequence_ids, first_time, last_time + 1});
            printOperation("query history overview", result.operation);
            std::cout << "total points: "
                      << result.overview.total_point_count << '\n';
            continue;
        }
        if (command == "2" || command == "3") {
            std::string sequence_id;
            std::cout << "sequence_id (for example ETTh1_OT)> ";
            std::cin >> sequence_id;
            if (command == "2") {
                const auto result = history.queryHistoryData(
                    {{sequence_id}, first_time, last_time + 1, std::nullopt});
                printOperation("query selected history", result.operation);
                const auto count = std::min<std::size_t>(
                    10, result.data.points.size());
                for (std::size_t index = 0; index < count; ++index) {
                    printPoint(result.data.points[index]);
                }
            } else {
                core::WindowQuery selected_window;
                selected_window.sequence_ids = {sequence_id};
                const auto result = window.queryWindowData(selected_window);
                printOperation("query selected hot window", result.operation);
                for (const auto& [id, points] : result.data.sequence_values) {
                    std::cout << id << " points: " << points.size() << '\n';
                }
            }
            continue;
        }
        std::cout << "unknown command; choose 1, 2, 3 or q\n";
    }
    return 0;
}
