#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "timeseries_core.grpc.pb.h"

namespace {

namespace pb = ::sfkg::timeseries::core::v1;
using Timestamp = std::int64_t;

const std::array<const char*, 7> kColumns{
    "HUFL", "HULL", "MUFL", "MULL", "LUFL", "LULL", "OT"};

struct Row {
    Timestamp time{0};
    std::array<double, 7> values{};
};

struct Options {
    std::string address{"127.0.0.1:50051"};
    std::string csv_path{"../ETTh1.csv"};
    std::size_t rows{50'000};
    std::size_t batch_rows{1'000};
    std::size_t workers{4};
    Timestamp window_size_ms{3'600'000};
    bool with_constraint{false};
    bool with_derived{false};
};

std::vector<std::string> split(const std::string& line) {
    std::vector<std::string> fields;
    std::stringstream stream(line);
    std::string field;
    while (std::getline(stream, field, ',')) {
        fields.push_back(field);
    }
    return fields;
}

bool fixedNumber(
    const std::string& text,
    std::size_t offset,
    std::size_t count,
    int* result) {
    if (offset + count > text.size()) {
        return false;
    }
    int value = 0;
    for (std::size_t index = offset; index < offset + count; ++index) {
        if (text[index] < '0' || text[index] > '9') {
            return false;
        }
        value = value * 10 + text[index] - '0';
    }
    *result = value;
    return true;
}

Timestamp daysFromCivil(int year, unsigned month, unsigned day) {
    year -= month <= 2;
    const int era = (year >= 0 ? year : year - 399) / 400;
    const unsigned year_of_era = static_cast<unsigned>(year - era * 400);
    const unsigned shifted_month = static_cast<unsigned>(
        static_cast<int>(month) + (month > 2 ? -3 : 9));
    const unsigned day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    const unsigned day_of_era = year_of_era * 365 + year_of_era / 4 -
        year_of_era / 100 + day_of_year;
    return static_cast<Timestamp>(era) * 146097 +
        static_cast<Timestamp>(day_of_era) - 719468;
}

bool parseTimestamp(const std::string& text, Timestamp* result) {
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
        !fixedNumber(text, 17, 2, &second)) {
        return false;
    }
    *result = daysFromCivil(year, static_cast<unsigned>(month),
                            static_cast<unsigned>(day)) * 86'400'000LL +
        static_cast<Timestamp>(hour) * 3'600'000LL +
        static_cast<Timestamp>(minute) * 60'000LL +
        static_cast<Timestamp>(second) * 1'000LL;
    return true;
}

bool loadRows(const Options& options, std::vector<Row>* rows) {
    std::ifstream input(options.csv_path);
    if (!input) {
        std::cerr << "cannot open ETTh1 CSV: " << options.csv_path << '\n';
        return false;
    }
    std::string line;
    if (!std::getline(input, line)) {
        std::cerr << "ETTh1 CSV is empty\n";
        return false;
    }
    const auto header = split(line);
    if (header.size() != kColumns.size() + 1 || header[0] != "date") {
        std::cerr << "unexpected ETTh1 header\n";
        return false;
    }
    for (std::size_t index = 0; index < kColumns.size(); ++index) {
        if (header[index + 1] != kColumns[index]) {
            std::cerr << "unexpected ETTh1 column: "
                      << header[index + 1] << '\n';
            return false;
        }
    }
    std::vector<Row> source_rows;
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }
        const auto fields = split(line);
        if (fields.size() != kColumns.size() + 1) {
            std::cerr << "invalid ETTh1 field count\n";
            return false;
        }
        Row row;
        if (!parseTimestamp(fields[0], &row.time)) {
            std::cerr << "invalid ETTh1 timestamp: " << fields[0] << '\n';
            return false;
        }
        try {
            for (std::size_t index = 0; index < kColumns.size(); ++index) {
                std::size_t consumed = 0;
                row.values[index] = std::stod(fields[index + 1], &consumed);
                if (consumed != fields[index + 1].size()) {
                    throw std::invalid_argument("trailing characters");
                }
            }
        } catch (const std::exception&) {
            std::cerr << "invalid ETTh1 value\n";
            return false;
        }
        source_rows.push_back(row);
    }
    if (source_rows.empty()) {
        std::cerr << "no ETTh1 rows selected\n";
        return false;
    }
    rows->reserve(options.rows);
    const auto base_time = std::chrono::duration_cast<
        std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    for (std::size_t index = 0; index < options.rows; ++index) {
        auto row = source_rows[index % source_rows.size()];
        // The source file is hourly ETTh1 data, but the Core acceptance test
        // models one timestamp per second. Reuse the real ETTh1 values while
        // assigning a fresh, strictly increasing 1-second timeline.
        row.time = base_time + static_cast<Timestamp>(index) * 1'000;
        rows->push_back(row);
    }
    return true;
}

bool positive(const std::string& text, std::size_t* result) {
    try {
        const auto value = std::stoull(text);
        if (value == 0) {
            return false;
        }
        *result = static_cast<std::size_t>(value);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool parseOptions(int argc, char* argv[], Options* options) {
    for (int index = 1; index < argc; ++index) {
        const std::string name = argv[index];
        if (name == "--help") {
            std::cout << "Usage: " << argv[0]
                      << " [--address ADDR] [--csv PATH] [--rows N]"
                      << " [--batch-rows N] [--workers N] [--window-ms N]"
                      << " [--with-constraint] [--with-derived]\n";
            return false;
        }
        if (name == "--with-constraint") {
            options->with_constraint = true;
            continue;
        }
        if (name == "--with-derived") {
            options->with_derived = true;
            continue;
        }
        if (index + 1 >= argc) {
            std::cerr << name << " requires a value\n";
            return false;
        }
        const std::string value = argv[++index];
        if (name == "--address") {
            options->address = value;
        } else if (name == "--csv") {
            options->csv_path = value;
        } else if (name == "--rows" || name == "--batch-rows" ||
                   name == "--workers") {
            std::size_t parsed = 0;
            if (!positive(value, &parsed)) {
                std::cerr << name << " requires a positive integer\n";
                return false;
            }
            if (name == "--rows") {
                options->rows = parsed;
            } else if (name == "--batch-rows") {
                options->batch_rows = parsed;
            } else {
                options->workers = parsed;
            }
        } else if (name == "--window-ms") {
            try {
                options->window_size_ms = std::stoll(value);
            } catch (const std::exception&) {
                std::cerr << "--window-ms requires an integer\n";
                return false;
            }
        } else {
            std::cerr << "unknown option: " << name << '\n';
            return false;
        }
    }
    return true;
}

bool syncCore(
    const std::shared_ptr<::grpc::Channel>& channel,
    Timestamp window_size_ms,
    bool with_constraint,
    bool with_derived) {
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    pb::SyncInstanceConfigsRequest instances;
    for (const auto* column : kColumns) {
        auto* item = instances.add_items();
        item->set_sequence_id(std::string("ETTh1_") + column);
        item->set_data_source_id("ETTh1.csv");
        item->set_external_sequence_id(column);
        item->set_category_id("ETTh1");
        item->set_data_type("double");
        item->set_series_kind(pb::SERIES_KIND_CONTINUOUS);
    }
    ::grpc::ClientContext instance_context;
    pb::SyncConfigResponse instance_response;
    const auto instance_status = stub->syncInstanceConfigs(
        &instance_context, instances, &instance_response);
    if (!instance_status.ok() ||
        instance_response.operation().code() != pb::OPERATION_CODE_OK) {
        std::cerr << "sync ETTh1 instances failed: "
                  << instance_response.operation().message() << '\n';
        return false;
    }

    pb::SyncWindowConfigRequest window;
    window.mutable_config()->set_window_size(window_size_ms);
    ::grpc::ClientContext window_context;
    pb::SyncConfigResponse window_response;
    const auto window_status = stub->syncWindowConfig(
        &window_context, window, &window_response);
    if (!window_status.ok() ||
        window_response.operation().code() != pb::OPERATION_CODE_OK) {
        std::cerr << "sync window config failed: "
                  << window_response.operation().message() << '\n';
        return false;
    }
    if (with_constraint) {
        pb::SyncConstraintsRequest constraints;
        auto* item = constraints.add_items();
        item->set_enabled(true);
        auto* rule = item->mutable_rule();
        rule->set_constraint_id("etth1-hot-diagnostic-range");
        (*rule->mutable_variable_mapping())["ot"] = "ETTh1_OT";
        rule->set_lower_bound(-1.0e12);
        rule->set_upper_bound(1.0e12);
        auto* term = rule->add_terms();
        term->set_variable("ot");
        term->set_coefficient(1.0);

        auto* multi_item = constraints.add_items();
        multi_item->set_enabled(true);
        auto* multi_rule = multi_item->mutable_rule();
        multi_rule->set_constraint_id("etth1-hot-multi-diagnostic-range");
        (*multi_rule->mutable_variable_mapping())["hufl"] = "ETTh1_HUFL";
        (*multi_rule->mutable_variable_mapping())["hull"] = "ETTh1_HULL";
        multi_rule->set_lower_bound(-1.0e12);
        multi_rule->set_upper_bound(1.0e12);
        auto* hufl_term = multi_rule->add_terms();
        hufl_term->set_variable("hufl");
        hufl_term->set_coefficient(1.0);
        auto* hull_term = multi_rule->add_terms();
        hull_term->set_variable("hull");
        hull_term->set_coefficient(1.0);
        ::grpc::ClientContext constraint_context;
        pb::SyncConfigResponse constraint_response;
        const auto constraint_status = stub->syncConstraints(
            &constraint_context, constraints, &constraint_response);
        if (!constraint_status.ok() ||
            constraint_response.operation().code() != pb::OPERATION_CODE_OK) {
            std::cerr << "sync diagnostic constraint failed: "
                      << constraint_response.operation().message() << '\n';
            return false;
        }
    }
    if (with_derived) {
        pb::SyncDerivedSeriesConfigsRequest derived_configs;
        auto* config = derived_configs.add_items();
        config->set_derived_sequence_id("ETTh1_OT_COPY");
        config->set_enabled(true);
        auto* linear = config->mutable_linear_combination();
        auto* term = linear->add_terms();
        term->set_sequence_id("ETTh1_OT");
        term->set_coefficient(1.0);
        ::grpc::ClientContext derived_context;
        pb::SyncConfigResponse derived_response;
        const auto derived_status = stub->syncDerivedSeriesConfigs(
            &derived_context, derived_configs, &derived_response);
        if (!derived_status.ok() ||
            derived_response.operation().code() != pb::OPERATION_CODE_OK) {
            std::cerr << "sync diagnostic derived config failed: "
                      << derived_response.operation().message() << '\n';
            return false;
        }
    }
    return true;
}

struct BenchmarkStats {
    std::atomic<std::uint64_t> submitted_points{0};
    std::atomic<std::uint64_t> persisted_points{0};
    std::atomic<std::uint64_t> failed_points{0};
    std::atomic<std::uint64_t> completed_rpcs{0};
    std::atomic<std::uint64_t> rpc_failures{0};
    std::mutex latency_mutex;
    std::vector<double> latencies_ms;
    std::mutex error_mutex;
    std::string first_error;
};

void recordError(BenchmarkStats* stats, const std::string& message) {
    std::lock_guard lock(stats->error_mutex);
    if (stats->first_error.empty()) {
        stats->first_error = message;
    }
}

void runWorker(
    std::size_t worker_id,
    const Options& options,
    const std::vector<Row>& rows,
    const std::shared_ptr<::grpc::Channel>& channel,
    BenchmarkStats* stats,
    std::atomic<bool>* start_flag) {
    while (!start_flag->load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    // Each sequence belongs to exactly one client worker. Thus the four
    // gRPC workers are concurrent, while every individual sequence still
    // receives batches in strictly increasing timestamp order.
    std::vector<std::size_t> columns;
    for (std::size_t column = worker_id;
         column < kColumns.size();
         column += options.workers) {
        columns.push_back(column);
    }
    if (columns.empty()) {
        return;
    }

    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    for (std::size_t offset = 0; offset < rows.size();
         offset += options.batch_rows) {
        const auto end = std::min(rows.size(), offset + options.batch_rows);
        pb::IngestDataRequest request;
        request.set_return_resolved_data(false);
        for (std::size_t row_index = offset; row_index < end; ++row_index) {
            for (const auto column : columns) {
                auto* point = request.add_points();
                point->set_sequence_id(
                    std::string("ETTh1_") + kColumns[column]);
                point->set_data_source_id("ETTh1.csv");
                point->set_external_sequence_id(kColumns[column]);
                point->set_time(rows[row_index].time);
                point->mutable_value()->set_double_value(
                    rows[row_index].values[column]);
            }
        }

        stats->submitted_points.fetch_add(
            request.points_size(), std::memory_order_relaxed);
        ::grpc::ClientContext context;
        context.set_wait_for_ready(true);
        context.set_deadline(
            std::chrono::system_clock::now() + std::chrono::seconds(60));
        pb::IngestDataResponse response;
        const auto started = std::chrono::steady_clock::now();
        const auto status = stub->ingestData(&context, request, &response);
        const auto elapsed = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - started).count();
        {
            std::lock_guard lock(stats->latency_mutex);
            stats->latencies_ms.push_back(elapsed);
        }
        stats->completed_rpcs.fetch_add(1, std::memory_order_relaxed);

        if (!status.ok()) {
            stats->rpc_failures.fetch_add(1, std::memory_order_relaxed);
            stats->failed_points.fetch_add(
                request.points_size(), std::memory_order_relaxed);
            recordError(stats, "worker " + std::to_string(worker_id) +
                " RPC failed: " + status.error_message());
            continue;
        }
        if (response.has_storage_result()) {
            stats->persisted_points.fetch_add(
                response.storage_result().success_count(),
                std::memory_order_relaxed);
            stats->failed_points.fetch_add(
                response.storage_result().failed_count(),
                std::memory_order_relaxed);
        } else {
            stats->failed_points.fetch_add(
                request.points_size(), std::memory_order_relaxed);
            recordError(stats, "worker " + std::to_string(worker_id) +
                " response has no storage result");
        }
        if (!response.has_operation() ||
            (response.operation().code() != pb::OPERATION_CODE_OK &&
             response.operation().code() != pb::OPERATION_CODE_PARTIAL_SUCCESS)) {
            recordError(stats, "worker " + std::to_string(worker_id) +
                " ingest operation failed: " +
                (response.has_operation()
                     ? response.operation().message()
                     : "response has no operation result"));
        }
    }
}

double percentile(
    std::vector<double> values,
    double fraction) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const auto index = static_cast<std::size_t>(
        fraction * static_cast<double>(values.size() - 1));
    return values[index];
}

int run(const Options& options, const std::vector<Row>& rows) {
    const auto channel = ::grpc::CreateChannel(
        options.address, ::grpc::InsecureChannelCredentials());
    if (!syncCore(
            channel,
            options.window_size_ms,
            options.with_constraint,
            options.with_derived)) {
        return 2;
    }
    std::cout << "ETTh1 gRPC ingest benchmark"
              << " rows=" << rows.size()
              << " batch_rows=" << options.batch_rows
              << " points=" << rows.size() * kColumns.size()
              << " workers=" << options.workers
              << " window_ms=" << options.window_size_ms
              << " constraint=" << (options.with_constraint ? "on" : "off")
              << " derived=" << (options.with_derived ? "on" : "off")
              << '\n';

    BenchmarkStats stats;
    std::atomic<bool> start_flag{false};
    std::vector<std::thread> workers;
    workers.reserve(options.workers);
    for (std::size_t worker = 0; worker < options.workers; ++worker) {
        workers.emplace_back(
            runWorker,
            worker,
            std::cref(options),
            std::cref(rows),
            std::cref(channel),
            &stats,
            &start_flag);
    }
    const auto started = std::chrono::steady_clock::now();
    start_flag.store(true, std::memory_order_release);
    for (auto& worker : workers) {
        worker.join();
    }
    const auto elapsed_seconds = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - started).count();
    std::vector<double> latencies;
    {
        std::lock_guard lock(stats.latency_mutex);
        latencies = stats.latencies_ms;
    }
    const auto persisted = stats.persisted_points.load();
    const auto submitted = stats.submitted_points.load();
    std::cout << std::fixed << std::setprecision(2)
              << "submitted_points=" << submitted
              << " persisted_points=" << persisted
              << " failed_points=" << stats.failed_points.load()
              << " rpc_failures=" << stats.rpc_failures.load() << '\n'
              << "elapsed_seconds=" << elapsed_seconds
              << " rpc_count=" << stats.completed_rpcs.load()
              << " rpc_latency_avg_ms="
              << (latencies.empty()
                      ? 0.0
                      : std::accumulate(
                            latencies.begin(), latencies.end(), 0.0) /
                          static_cast<double>(latencies.size()))
              << " rpc_latency_p50_ms=" << percentile(latencies, 0.50)
              << " rpc_latency_p95_ms=" << percentile(latencies, 0.95)
              << '\n'
              << "throughput_rows_per_sec="
              << (elapsed_seconds == 0.0
                      ? 0.0
                      : static_cast<double>(persisted) /
                          static_cast<double>(kColumns.size()) /
                          elapsed_seconds)
              << " throughput_points_per_sec="
              << (elapsed_seconds == 0.0
                      ? 0.0
                      : static_cast<double>(persisted) / elapsed_seconds)
              << '\n';
    if (!stats.first_error.empty()) {
        std::cerr << stats.first_error << '\n';
    }
    return stats.rpc_failures.load() == 0 &&
            stats.failed_points.load() == 0
        ? 0
        : 3;
}

}  // namespace

int main(int argc, char* argv[]) {
    Options options;
    if (!parseOptions(argc, argv, &options)) {
        return argc > 1 && std::string(argv[1]) == "--help" ? 0 : 1;
    }
    if (options.rows == 0 || options.batch_rows == 0 ||
        options.workers == 0 || options.window_size_ms <= 0) {
        std::cerr << "rows, batch-rows, workers and window-ms must be positive\n";
        return 1;
    }
    std::vector<Row> rows;
    if (!loadRows(options, &rows)) {
        return 1;
    }
    return run(options, rows);
}
