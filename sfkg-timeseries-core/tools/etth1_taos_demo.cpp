#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "sfkg/timeseries/core/history_query_service.hpp"
#include "sfkg/timeseries/core/internal/taos_client.hpp"

namespace core = sfkg::timeseries::core;

namespace {

const std::vector<std::string> kColumns{
    "HUFL", "HULL", "MUFL", "MULL", "LUFL", "LULL", "OT"};

const char* operationName(core::OperationCode code) {
    switch (code) {
    case core::OperationCode::Ok:
        return "OK";
    case core::OperationCode::PartialSuccess:
        return "PARTIAL_SUCCESS";
    case core::OperationCode::InvalidArgument:
        return "INVALID_ARGUMENT";
    case core::OperationCode::NotFound:
        return "NOT_FOUND";
    case core::OperationCode::FailedPrecondition:
        return "FAILED_PRECONDITION";
    case core::OperationCode::Unavailable:
        return "UNAVAILABLE";
    case core::OperationCode::InternalError:
        return "INTERNAL_ERROR";
    case core::OperationCode::NotImplemented:
        return "NOT_IMPLEMENTED";
    }
    return "UNKNOWN";
}

void printOperation(const char* name, const core::OperationResult& result) {
    std::cout << name << ": " << operationName(result.code)
              << " success=" << result.success_count
              << " failed=" << result.failed_count
              << " message=" << result.message << '\n';
}

bool fixedNumber(std::string_view text, std::size_t offset, std::size_t count,
                 int* value) {
    if (offset + count > text.size()) {
        return false;
    }
    int parsed = 0;
    for (std::size_t index = offset; index < offset + count; ++index) {
        if (text[index] < '0' || text[index] > '9') {
            return false;
        }
        parsed = parsed * 10 + (text[index] - '0');
    }
    *value = parsed;
    return true;
}

// Days from civil date, relative to 1970-01-01. This avoids using the local
// timezone when converting the CSV timestamps to TDengine millisecond time.
std::int64_t daysFromCivil(int year, unsigned month, unsigned day) {
    year -= month <= 2;
    const int era = (year >= 0 ? year : year - 399) / 400;
    const unsigned yearOfEra = static_cast<unsigned>(year - era * 400);
    const unsigned shiftedMonth = static_cast<unsigned>(
        static_cast<int>(month) + (month > 2 ? -3 : 9));
    const unsigned dayOfYear =
        (153 * shiftedMonth + 2) / 5 + day - 1;
    const unsigned dayOfEra = yearOfEra * 365 + yearOfEra / 4 -
        yearOfEra / 100 + dayOfYear;
    return static_cast<std::int64_t>(era) * 146097 +
        static_cast<std::int64_t>(dayOfEra) - 719468;
}

bool parseTimestamp(std::string_view text, core::Timestamp* timestamp) {
    if (text.size() != 19 || text[4] != '-' || text[7] != '-' ||
        text[10] != ' ' || text[13] != ':' || text[16] != ':') {
        return false;
    }
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
        !fixedNumber(text, 17, 2, &second) || month < 1 || month > 12 ||
        day < 1 || day > 31 || hour > 23 || minute > 59 || second > 59) {
        return false;
    }
    *timestamp = daysFromCivil(year, static_cast<unsigned>(month),
                               static_cast<unsigned>(day)) * 86'400'000LL +
        static_cast<core::Timestamp>(hour) * 3'600'000LL +
        static_cast<core::Timestamp>(minute) * 60'000LL +
        static_cast<core::Timestamp>(second) * 1'000LL;
    return true;
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
    output << std::put_time(&utc, "%Y-%m-%d %H:%M:%S") << '.'
           << std::setw(3) << std::setfill('0') << timestamp % 1'000;
    return output.str();
}

void printValue(const core::TimeseriesValue& value) {
    std::visit([](const auto& item) { std::cout << item; }, value);
}

void printPoint(const core::RawTimeseriesPoint& point) {
    std::cout << "  " << formatTimestamp(point.time) << " (" << point.time
              << ") " << point.sequence_id << " = ";
    printValue(point.value);
    std::cout << '\n';
}

std::string datasetNameFromPath(const std::string& path) {
    const std::string name = std::filesystem::path(path).stem().string();
    return name.empty() ? "ETT" : name;
}

std::string databasePart(const std::string& dataset_name) {
    std::string result;
    result.reserve(dataset_name.size());
    for (const char ch : dataset_name) {
        if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
            (ch >= '0' && ch <= '9') || ch == '_') {
            result.push_back(static_cast<char>(
                ch >= 'A' && ch <= 'Z' ? ch - 'A' + 'a' : ch));
        } else {
            result.push_back('_');
        }
    }
    return result.empty() ? "ett" : result;
}

bool setDefaultEnvironment(const std::string& dataset_name) {
    if (std::getenv("SFKG_TAOS_DB") == nullptr) {
        const std::string database =
            "sfkg_" + databasePart(dataset_name) + "_demo";
        setenv("SFKG_TAOS_DB", database.c_str(), 1);
    }
    if (std::getenv("SFKG_TAOS_KEEP_DAYS") == nullptr) {
        setenv("SFKG_TAOS_KEEP_DAYS", "20000", 1);
    }
    return true;
}

}  // namespace

int main(int argc, char* argv[]) {
    const std::string path = argc > 1 ? argv[1] : "ETTh1.csv";
    const std::string dataset_name = datasetNameFromPath(path);
    setDefaultEnvironment(dataset_name);
    constexpr std::size_t printLimit = 12;

    std::ifstream input(path);
    if (!input) {
        std::cerr << "cannot open CSV: " << path << '\n';
        return 1;
    }

    std::string line;
    if (!std::getline(input, line)) {
        std::cerr << "CSV is empty\n";
        return 1;
    }
    const auto header = splitCsvLine(line);
    if (header.size() != kColumns.size() + 1 || header[0] != "date") {
        std::cerr << "unexpected ETT header; expected date plus 7 columns\n";
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
    sequence_ids.reserve(kColumns.size());
    for (const auto& column : kColumns) {
        sequence_ids.push_back(dataset_name + "_" + column);
    }

    core::TimeseriesBatch batch;
    core::Timestamp firstTime = std::numeric_limits<core::Timestamp>::max();
    core::Timestamp lastTime = std::numeric_limits<core::Timestamp>::min();
    std::size_t csvRows = 0;
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }
        const auto fields = splitCsvLine(line);
        if (fields.size() != kColumns.size() + 1) {
            std::cerr << "invalid field count at CSV row " << csvRows + 2
                      << '\n';
            return 1;
        }
        core::Timestamp timestamp = 0;
        if (!parseTimestamp(fields[0], &timestamp)) {
            std::cerr << "invalid timestamp at CSV row " << csvRows + 2
                      << ": " << fields[0] << '\n';
            return 1;
        }
        firstTime = std::min(firstTime, timestamp);
        lastTime = std::max(lastTime, timestamp);
        for (std::size_t index = 0; index < kColumns.size(); ++index) {
            double value = 0.0;
            if (!parseDouble(fields[index + 1], &value)) {
                std::cerr << "invalid number at CSV row " << csvRows + 2
                          << ", column " << kColumns[index] << '\n';
                return 1;
            }
            batch.points.push_back(
                {timestamp, sequence_ids[index], value});
        }
        ++csvRows;
    }
    if (batch.points.empty()) {
        std::cerr << "CSV contains no data rows\n";
        return 1;
    }

    std::cout << "CSV: " << path << '\n'
              << "rows: " << csvRows << ", points to insert: "
              << batch.points.size() << '\n'
              << "time range: " << formatTimestamp(firstTime) << " .. "
              << formatTimestamp(lastTime) << "\n"
              << "database: "
              << (std::getenv("SFKG_TAOS_DB") != nullptr
                      ? std::getenv("SFKG_TAOS_DB")
                      : "sfkg_etth1_demo")
              << "\n\n";

    core::internal::TaosClient client;
    printOperation("ensureSchema", client.ensureSchema());
    const auto write = client.insertRaw(batch);
    printOperation("direct TaosClient insertRaw (not gRPC WriteRawData)", write);
    if (write.code != core::OperationCode::Ok) {
        return 1;
    }

    core::RuntimeConfigRegistry registry;
    core::RuntimeConfigSnapshot<core::RuntimeInstanceConfig> snapshot;
    for (const auto& sequence_id : sequence_ids) {
        snapshot.items.push_back(
            {sequence_id, path, sequence_id, dataset_name, "double"});
    }
    const auto registered = registry.replaceInstanceConfigs(snapshot);
    printOperation("register temporary sequences", registered);
    if (registered.code != core::OperationCode::Ok) {
        return 1;
    }

    core::HistoryQueryService history(registry, client);
    const core::HistoryOverviewQuery overviewQuery{
        sequence_ids, firstTime, lastTime + 1};
    const auto overview = history.queryHistoryOverview(overviewQuery);
    printOperation("queryHistoryOverview", overview.operation);
    std::cout << "overview: sequences=" << overview.overview.sequence_count
              << ", points=" << overview.overview.total_point_count << '\n';
    for (const auto& series : overview.overview.series) {
        std::cout << "  " << series.sequence_id
                  << ": " << series.point_count << " points, "
                  << formatTimestamp(*series.first_time) << " .. "
                  << formatTimestamp(*series.last_time) << '\n';
    }

    const core::HistoryQuery dataQuery{
        sequence_ids, firstTime, lastTime + 1, std::nullopt};
    const auto data = history.queryHistoryData(dataQuery);
    printOperation("queryHistoryData", data.operation);
    std::cout << "query returned: " << data.data.points.size() << " points\n";
    const std::size_t headCount = std::min(printLimit, data.data.points.size());
    std::cout << "first " << headCount << " points:\n";
    for (std::size_t index = 0; index < headCount; ++index) {
        printPoint(data.data.points[index]);
    }
    const std::size_t tailStart = data.data.points.size() > printLimit
        ? data.data.points.size() - printLimit
        : headCount;
    if (tailStart < data.data.points.size()) {
        std::cout << "last " << data.data.points.size() - tailStart
                  << " points:\n";
        for (std::size_t index = tailStart; index < data.data.points.size();
             ++index) {
            printPoint(data.data.points[index]);
        }
    }

    const bool passed = overview.operation.code == core::OperationCode::Ok &&
        overview.overview.total_point_count == batch.points.size() &&
        data.operation.code == core::OperationCode::Ok &&
        data.data.points.size() == batch.points.size();
    std::cout << (passed ? "\n" + dataset_name + " demo passed\n"
                         : "\n" + dataset_name + " demo failed\n");
    return passed ? 0 : 1;
}
