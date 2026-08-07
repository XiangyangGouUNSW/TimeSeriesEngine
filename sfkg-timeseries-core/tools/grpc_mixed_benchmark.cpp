#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "timeseries_core.grpc.pb.h"

namespace fs = std::filesystem;

using SteadyClock = std::chrono::steady_clock;
using SystemClock = std::chrono::system_clock;

namespace {

namespace pb = ::sfkg::timeseries::core::v1;

struct Options {
    std::string address{"127.0.0.1:50051"};
    std::string run_id;
    std::size_t ingest_workers{6};
    std::size_t query_workers{2};
    std::size_t batch_size{1000};
    std::size_t sequences_per_worker{4};
    std::uint64_t duration_seconds{30};
    std::int64_t window_size_ms{3'600'000};
    std::uint64_t report_interval_seconds{5};
    std::string output_file{
        "tests/benchmarks/results/grpc_mixed_benchmark.csv"};
    std::string log_file{
        "tests/benchmarks/results/grpc_mixed_benchmark.log"};
};

class Logger {
public:
    explicit Logger(const std::string& path) {
        if (path.empty()) {
            return;
        }
        const fs::path file_path(path);
        std::error_code error;
        if (!file_path.parent_path().empty()) {
            fs::create_directories(file_path.parent_path(), error);
        }
        if (!error) {
            file_.open(path, std::ios::out | std::ios::trunc);
        }
    }

    void line(const std::string& message) {
        std::lock_guard lock(mutex_);
        std::cout << message << '\n';
        if (file_.is_open()) {
            file_ << message << '\n';
            file_.flush();
        }
    }

private:
    std::mutex mutex_;
    std::ofstream file_;
};

struct CallStats {
    std::atomic<std::uint64_t> calls{0};
    std::atomic<std::uint64_t> successes{0};
    std::atomic<std::uint64_t> failures{0};
    mutable std::mutex latency_mutex;
    std::vector<double> latency_ms;

    void record(bool success, double milliseconds) {
        calls.fetch_add(1, std::memory_order_relaxed);
        (success ? successes : failures).fetch_add(
            1, std::memory_order_relaxed);
        std::lock_guard lock(latency_mutex);
        if (latency_ms.size() < 100'000) {
            latency_ms.push_back(milliseconds);
        }
    }
};

struct Stats {
    std::atomic<std::uint64_t> submitted_points{0};
    std::atomic<std::uint64_t> persisted_points{0};
    std::atomic<std::uint64_t> storage_failed_points{0};
    std::atomic<std::uint64_t> window_failed_points{0};
    std::atomic<std::uint64_t> rpc_failures{0};
    std::mutex error_mutex;
    std::size_t error_samples{0};

    CallStats history_data;
    CallStats history_overview;
    CallStats window_data;
    CallStats alignment;
    CallStats statistics;
    CallStats constraints;

    bool acceptErrorSample() {
        std::lock_guard lock(error_mutex);
        if (error_samples >= 5) {
            return false;
        }
        ++error_samples;
        return true;
    }
};

std::string operationCode(pb::OperationCode code) {
    return pb::OperationCode_Name(code);
}

bool isSuccessful(pb::OperationCode code) {
    return code == pb::OPERATION_CODE_OK ||
           code == pb::OPERATION_CODE_PARTIAL_SUCCESS;
}

bool parsePositive(
    const std::string& text,
    std::uint64_t* output,
    const char* option) {
    if (text.empty()) {
        std::cerr << option << " requires a positive integer\n";
        return false;
    }
    std::uint64_t value = 0;
    for (const char ch : text) {
        if (ch < '0' || ch > '9') {
            std::cerr << option << " requires a positive integer\n";
            return false;
        }
        const auto digit = static_cast<std::uint64_t>(ch - '0');
        value = value * 10 + digit;
    }
    if (value == 0) {
        std::cerr << option << " requires a positive integer\n";
        return false;
    }
    *output = value;
    return true;
}

void usage(const char* program) {
    std::cout
        << "Usage: " << program << " [options]\n"
        << "  --address ADDR                 Core address\n"
        << "  --run-id ID                    unique sequence namespace\n"
        << "  --ingest-workers N             concurrent ingest threads (default 6)\n"
        << "  --query-workers N              concurrent mixed query threads (default 2)\n"
        << "  --batch-size N                 points per ingestData RPC (default 1000)\n"
        << "  --sequences-per-worker N       ingest sequences per worker (default 4)\n"
        << "  --duration-sec N               measured duration (default 30)\n"
        << "  --window-size-ms N             Core hot-window size (default 3600000)\n"
        << "  --report-interval-sec N        progress interval (default 5)\n"
        << "  --output PATH                  CSV result path\n"
        << "  --log-file PATH                summary log path\n"
        << "  --help                         show this help\n";
}

bool nextValue(
    int argc,
    char* argv[],
    int* index,
    const char* option,
    std::string* value) {
    if (*index + 1 >= argc) {
        std::cerr << option << " requires a value\n";
        return false;
    }
    *value = argv[++(*index)];
    return true;
}

bool parseOptions(int argc, char* argv[], Options* options) {
    for (int index = 1; index < argc; ++index) {
        const std::string argument = argv[index];
        std::string value;
        if (argument == "--help") {
            usage(argv[0]);
            return false;
        }
        if (argument == "--address") {
            if (!nextValue(argc, argv, &index, "--address", &options->address)) {
                return false;
            }
            continue;
        }
        if (argument == "--run-id") {
            if (!nextValue(argc, argv, &index, "--run-id", &options->run_id)) {
                return false;
            }
            continue;
        }
        if (argument == "--output") {
            if (!nextValue(argc, argv, &index, "--output", &options->output_file)) {
                return false;
            }
            continue;
        }
        if (argument == "--log-file") {
            if (!nextValue(argc, argv, &index, "--log-file", &options->log_file)) {
                return false;
            }
            continue;
        }

        std::uint64_t parsed = 0;
        if (!nextValue(argc, argv, &index, argument.c_str(), &value) ||
            !parsePositive(value, &parsed, argument.c_str())) {
            return false;
        }
        if (argument == "--ingest-workers") {
            options->ingest_workers = static_cast<std::size_t>(parsed);
        } else if (argument == "--query-workers") {
            options->query_workers = static_cast<std::size_t>(parsed);
        } else if (argument == "--batch-size") {
            options->batch_size = static_cast<std::size_t>(parsed);
        } else if (argument == "--sequences-per-worker") {
            options->sequences_per_worker = static_cast<std::size_t>(parsed);
        } else if (argument == "--duration-sec") {
            options->duration_seconds = parsed;
        } else if (argument == "--window-size-ms") {
            options->window_size_ms = static_cast<std::int64_t>(parsed);
        } else if (argument == "--report-interval-sec") {
            options->report_interval_seconds = parsed;
        } else {
            std::cerr << "unknown option: " << argument << '\n';
            usage(argv[0]);
            return false;
        }
    }
    return true;
}

std::vector<std::string> makeSequenceIds(const Options& options) {
    std::vector<std::string> ids;
    ids.reserve(options.ingest_workers * options.sequences_per_worker);
    for (std::size_t worker = 0; worker < options.ingest_workers; ++worker) {
        for (std::size_t sequence = 0;
             sequence < options.sequences_per_worker;
             ++sequence) {
            ids.push_back(
                "grpc_mixed_r" + options.run_id +
                "_w" + std::to_string(worker) +
                "_s" + std::to_string(sequence));
        }
    }
    return ids;
}

bool setupConfigs(
    const std::shared_ptr<::grpc::Channel>& channel,
    const std::vector<std::string>& ids,
    Logger* logger) {
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    const auto call = [&](const char* name, auto&& invoke) {
        const auto started = SteadyClock::now();
        const auto result = invoke(*stub);
        const auto elapsed = std::chrono::duration<double, std::milli>(
            SteadyClock::now() - started).count();
        logger->line(
            std::string("[mixed] setup ") + name +
            " grpc=" + std::to_string(result.first.error_code()) +
            " operation=" + operationCode(result.second) +
            " elapsed_ms=" + [&] {
                std::ostringstream value;
                value << std::fixed << std::setprecision(2) << elapsed;
                return value.str();
            }());
        return result.first.ok() && isSuccessful(result.second);
    };

    pb::SyncInstanceConfigsRequest instances;
    for (const auto& id : ids) {
        auto* item = instances.add_items();
        item->set_sequence_id(id);
        item->set_data_source_id("grpc-mixed-benchmark");
        item->set_external_sequence_id(id);
        item->set_category_id("benchmark");
        item->set_data_type("double");
        item->set_series_kind(pb::SERIES_KIND_CONTINUOUS);
    }
    if (!call("syncInstanceConfigs", [&](auto& stub) {
            ::grpc::ClientContext context;
            pb::SyncConfigResponse response;
            const auto status = stub.syncInstanceConfigs(
                &context, instances, &response);
            return std::make_pair(status, response.operation().code());
        })) {
        return false;
    }

    pb::SyncRelationsRequest relations;
    auto* relation = relations.add_items();
    relation->set_relation_id("grpc-mixed-relation");
    relation->set_target_sequence_id(ids.at(0));
    relation->set_relation_type("correlation");
    relation->set_confidence(1.0);
    relation->set_enabled(true);
    auto* source = relation->add_sources();
    source->set_source_sequence_id(ids.at(1));
    source->set_weight(1.0);
    source->set_fixed_lag(0);
    if (!call("syncRelations", [&](auto& stub) {
            ::grpc::ClientContext context;
            pb::SyncConfigResponse response;
            const auto status = stub.syncRelations(
                &context, relations, &response);
            return std::make_pair(status, response.operation().code());
        })) {
        return false;
    }

    pb::SyncConstraintsRequest constraints;
    auto* constraint = constraints.add_items();
    constraint->set_enabled(true);
    auto* rule = constraint->mutable_rule();
    rule->set_constraint_id("grpc-mixed-range");
    (*rule->mutable_variable_mapping())["x"] = ids.at(0);
    rule->set_lower_bound(-1.0e12);
    rule->set_upper_bound(1.0e12);
    auto* term = rule->add_terms();
    term->set_variable("x");
    term->set_coefficient(1.0);
    term->set_sample_offset(0);
    return call("syncConstraints", [&](auto& stub) {
        ::grpc::ClientContext context;
        pb::SyncConfigResponse response;
        const auto status = stub.syncConstraints(
            &context, constraints, &response);
        return std::make_pair(status, response.operation().code());
    });
}

void recordFailure(
    const char* name,
    const ::grpc::Status& status,
    pb::OperationCode code,
    const std::string& operation_message,
    Stats* stats,
    Logger* logger) {
    stats->rpc_failures.fetch_add(1, std::memory_order_relaxed);
    if (stats->acceptErrorSample()) {
        logger->line(
            std::string("[mixed] ") + name +
            " failed grpc=" + std::to_string(status.error_code()) +
            " operation=" + operationCode(code) +
            " message=" + (operation_message.empty()
                ? status.error_message()
                : operation_message));
    }
}

template <typename Response>
bool operationSucceeded(
    const ::grpc::Status& status,
    const Response& response) {
    return status.ok() && isSuccessful(response.operation().code());
}

void ingestMain(
    std::size_t worker_id,
    const Options& options,
    const std::shared_ptr<::grpc::Channel>& channel,
    const std::vector<std::string>& worker_ids,
    std::int64_t base_timestamp,
    SteadyClock::time_point start_time,
    Stats* stats,
    Logger* logger,
    std::atomic<bool>* start_flag) {
    while (!start_flag->load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    const auto end_time = start_time +
        std::chrono::seconds(options.duration_seconds);
    const auto sequence_count = worker_ids.size();
    const auto points_per_sequence =
        (options.batch_size + sequence_count - 1) / sequence_count;
    std::uint64_t batch_index = 0;

    while (SteadyClock::now() < end_time) {
        pb::IngestDataRequest request;
        request.set_window_size(options.window_size_ms);
        for (std::size_t point_index = 0;
             point_index < options.batch_size;
             ++point_index) {
            const auto sequence_index = point_index % sequence_count;
            const auto local_index = point_index / sequence_count;
            const auto offset = static_cast<std::int64_t>(
                (batch_index * points_per_sequence + local_index) *
                    sequence_count + sequence_index);
            auto* point = request.add_points();
            point->set_sequence_id(worker_ids[sequence_index]);
            point->set_data_source_id("grpc-mixed-benchmark");
            point->set_external_sequence_id(worker_ids[sequence_index]);
            point->set_time(base_timestamp + offset);
            point->mutable_value()->set_double_value(
                static_cast<double>(worker_id) +
                static_cast<double>(batch_index * options.batch_size + point_index) *
                    0.001);
        }

        stats->submitted_points.fetch_add(
            request.points_size(), std::memory_order_relaxed);
        ::grpc::ClientContext context;
        context.set_wait_for_ready(true);
        context.set_deadline(
            std::chrono::system_clock::now() + std::chrono::seconds(15));
        pb::IngestDataResponse response;
        const auto status = stub->ingestData(&context, request, &response);
        if (!status.ok()) {
            recordFailure(
                "ingestData", status,
                response.has_operation()
                    ? response.operation().code()
                    : pb::OPERATION_CODE_INTERNAL_ERROR,
                response.has_operation() ? response.operation().message() : "",
                stats, logger);
        } else {
            if (response.has_storage_result()) {
                stats->persisted_points.fetch_add(
                    response.storage_result().success_count(),
                    std::memory_order_relaxed);
                stats->storage_failed_points.fetch_add(
                    response.storage_result().failed_count(),
                    std::memory_order_relaxed);
            } else {
                stats->storage_failed_points.fetch_add(
                    request.points_size(), std::memory_order_relaxed);
            }
            if (response.has_window_result()) {
                stats->window_failed_points.fetch_add(
                    response.window_result().failed_count(),
                    std::memory_order_relaxed);
            } else {
                stats->window_failed_points.fetch_add(
                    request.points_size(), std::memory_order_relaxed);
            }
            if (!operationSucceeded(status, response)) {
                recordFailure(
                    "ingestData", status, response.operation().code(),
                    response.operation().message(), stats, logger);
            }
        }
        ++batch_index;
    }
}

pb::QueryWindowDataRequest windowQuery(
    const std::vector<std::string>& ids,
    std::int64_t start_time,
    std::int64_t end_time) {
    pb::QueryWindowDataRequest request;
    for (const auto& id : ids) {
        request.add_sequence_ids(id);
    }
    request.set_start_time(start_time);
    request.set_end_time(end_time);
    return request;
}

pb::AlignmentConfig alignmentConfig(const std::vector<std::string>& ids) {
    pb::AlignmentConfig config;
    config.set_bucket_interval(1000);
    auto* independent = config.add_sequences();
    independent->set_sequence_id(ids.at(0));
    independent->set_role(pb::VARIABLE_ROLE_INDEPENDENT);
    independent->set_aggregation(pb::BUCKET_AGGREGATION_AVERAGE);
    independent->set_fill_method(pb::GAP_FILL_METHOD_PREVIOUS);
    auto* dependent = config.add_sequences();
    dependent->set_sequence_id(ids.at(1));
    dependent->set_role(pb::VARIABLE_ROLE_DEPENDENT);
    dependent->set_aggregation(pb::BUCKET_AGGREGATION_AVERAGE);
    dependent->set_fill_method(pb::GAP_FILL_METHOD_PREVIOUS);
    return config;
}

void recordQueryResult(
    const char* name,
    const ::grpc::Status& status,
    pb::OperationCode code,
    double elapsed_ms,
    bool success,
    CallStats* operation_stats,
    Stats* stats,
    Logger* logger) {
    operation_stats->record(success, elapsed_ms);
    if (!success) {
        recordFailure(name, status, code, "", stats, logger);
    }
}

void queryMain(
    const Options& options,
    const std::shared_ptr<::grpc::Channel>& channel,
    const std::vector<std::string>& all_ids,
    std::int64_t base_timestamp,
    SteadyClock::time_point start_time,
    Stats* stats,
    Logger* logger,
    std::atomic<bool>* start_flag) {
    while (!start_flag->load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    // Avoid counting empty-window INVALID_ARGUMENT results during startup.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    const auto end_time = start_time +
        std::chrono::seconds(options.duration_seconds);
    const std::vector<std::string> query_ids{all_ids.at(0), all_ids.at(1)};
    // Keep response bodies small enough for the default gRPC message limit
    // while still exercising the complete query/align/compute/check path.
    const auto query_start_time = base_timestamp;
    const auto query_end_time = base_timestamp + 10'000;
    const auto config = alignmentConfig(query_ids);
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    std::uint64_t iteration = 0;

    while (SteadyClock::now() < end_time) {
        {
            pb::QueryHistoryDataRequest request;
            request.add_sequence_ids(query_ids[0]);
            request.add_sequence_ids(query_ids[1]);
            request.set_start_time(query_start_time);
            request.set_end_time(query_end_time);
            request.set_granularity(1000);
            pb::QueryHistoryDataResponse response;
            ::grpc::ClientContext context;
            context.set_wait_for_ready(true);
            context.set_deadline(
                std::chrono::system_clock::now() + std::chrono::seconds(15));
            const auto started = SteadyClock::now();
            const auto status = stub->queryHistoryData(
                &context, request, &response);
            const auto elapsed = std::chrono::duration<double, std::milli>(
                SteadyClock::now() - started).count();
            recordQueryResult(
                "queryHistoryData", status, response.operation().code(),
                elapsed, operationSucceeded(status, response),
                &stats->history_data, stats, logger);
        }

        {
            pb::QueryHistoryOverviewRequest request;
            request.add_sequence_ids(query_ids[0]);
            request.add_sequence_ids(query_ids[1]);
            pb::QueryHistoryOverviewResponse response;
            ::grpc::ClientContext context;
            context.set_wait_for_ready(true);
            context.set_deadline(
                std::chrono::system_clock::now() + std::chrono::seconds(15));
            const auto started = SteadyClock::now();
            const auto status = stub->queryHistoryOverview(
                &context, request, &response);
            const auto elapsed = std::chrono::duration<double, std::milli>(
                SteadyClock::now() - started).count();
            recordQueryResult(
                "queryHistoryOverview", status, response.operation().code(),
                elapsed, operationSucceeded(status, response),
                &stats->history_overview, stats, logger);
        }

        {
            auto request = windowQuery(
                query_ids, query_start_time, query_end_time);
            pb::QueryWindowDataResponse response;
            ::grpc::ClientContext context;
            context.set_wait_for_ready(true);
            context.set_deadline(
                std::chrono::system_clock::now() + std::chrono::seconds(15));
            const auto started = SteadyClock::now();
            const auto status = stub->queryWindowData(
                &context, request, &response);
            const auto elapsed = std::chrono::duration<double, std::milli>(
                SteadyClock::now() - started).count();
            recordQueryResult(
                "queryWindowData", status, response.operation().code(),
                elapsed, operationSucceeded(status, response),
                &stats->window_data, stats, logger);
        }

        {
            pb::ComputeStatisticsRequest request;
            *request.mutable_window_query() = windowQuery(
                query_ids, query_start_time, query_end_time);
            pb::ComputeStatisticsResponse response;
            ::grpc::ClientContext context;
            context.set_wait_for_ready(true);
            context.set_deadline(
                std::chrono::system_clock::now() + std::chrono::seconds(15));
            const auto started = SteadyClock::now();
            const auto status = stub->computeBasicStatistics(
                &context, request, &response);
            const auto elapsed = std::chrono::duration<double, std::milli>(
                SteadyClock::now() - started).count();
            recordQueryResult(
                "computeBasicStatistics", status, response.operation().code(),
                elapsed, operationSucceeded(status, response),
                &stats->statistics, stats, logger);
        }

        pb::AlignedWindowData aligned_data;
        bool aligned = false;
        {
            pb::AlignWindowDataRequest request;
            *request.mutable_window_query() = windowQuery(
                query_ids, query_start_time, query_end_time);
            *request.mutable_config() = config;
            request.add_relation_ids("grpc-mixed-relation");
            pb::AlignWindowDataResponse response;
            ::grpc::ClientContext context;
            context.set_wait_for_ready(true);
            context.set_deadline(
                std::chrono::system_clock::now() + std::chrono::seconds(15));
            const auto started = SteadyClock::now();
            const auto status = stub->alignWindowData(
                &context, request, &response);
            const auto elapsed = std::chrono::duration<double, std::milli>(
                SteadyClock::now() - started).count();
            const bool success = operationSucceeded(status, response);
            recordQueryResult(
                "alignWindowData", status, response.operation().code(),
                elapsed, success, &stats->alignment, stats, logger);
            if (success && response.has_aligned_data()) {
                aligned_data = response.aligned_data();
                aligned = true;
            }
        }

        {
            pb::CheckConstraintsRequest request;
            request.add_constraint_ids("grpc-mixed-range");
            *request.mutable_window_query() = windowQuery(
                query_ids, query_start_time, query_end_time);
            pb::CheckConstraintsResponse response;
            ::grpc::ClientContext context;
            context.set_wait_for_ready(true);
            context.set_deadline(
                std::chrono::system_clock::now() + std::chrono::seconds(15));
            const auto started = SteadyClock::now();
            const auto status = stub->checkConstraints(
                &context, request, &response);
            const auto elapsed = std::chrono::duration<double, std::milli>(
                SteadyClock::now() - started).count();
            recordQueryResult(
                "checkConstraints", status, response.operation().code(),
                elapsed, operationSucceeded(status, response),
                &stats->constraints, stats, logger);
        }

        if (aligned) {
            pb::ComputeStatisticsRequest request;
            *request.mutable_aligned_data() = aligned_data;
            request.set_relation_id("grpc-mixed-relation");
            pb::ComputeStatisticsResponse response;
            ::grpc::ClientContext context;
            context.set_wait_for_ready(true);
            context.set_deadline(
                std::chrono::system_clock::now() + std::chrono::seconds(15));
            const auto started = SteadyClock::now();
            const auto status = stub->computeBasicStatistics(
                &context, request, &response);
            const auto elapsed = std::chrono::duration<double, std::milli>(
                SteadyClock::now() - started).count();
            recordQueryResult(
                "computeBasicStatistics(aligned)", status,
                response.operation().code(), elapsed,
                operationSucceeded(status, response),
                &stats->statistics, stats, logger);
        }
        ++iteration;
    }
}

double percentile(std::vector<double> values, double fraction) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const auto index = static_cast<std::size_t>(
        fraction * static_cast<double>(values.size() - 1));
    return values[index];
}

void logCallStats(const char* name, const CallStats& stats, Logger* logger) {
    std::vector<double> latency;
    {
        std::lock_guard lock(stats.latency_mutex);
        latency = stats.latency_ms;
    }
    const double average = latency.empty()
        ? 0.0
        : std::accumulate(latency.begin(), latency.end(), 0.0) /
            static_cast<double>(latency.size());
    std::ostringstream line;
    line << std::fixed << std::setprecision(2)
         << "[mixed] rpc=" << name
         << " calls=" << stats.calls.load()
         << " successes=" << stats.successes.load()
         << " failures=" << stats.failures.load()
         << " latency_avg_ms=" << average
         << " p50_ms=" << percentile(latency, 0.50)
         << " p95_ms=" << percentile(latency, 0.95)
         << " p99_ms=" << percentile(latency, 0.99);
    logger->line(line.str());
}

}  // namespace

int main(int argc, char* argv[]) {
    Options options;
    if (!parseOptions(argc, argv, &options)) {
        return argc > 1 && std::string(argv[1]) == "--help" ? 0 : 1;
    }
    if (options.ingest_workers == 0 || options.query_workers == 0 ||
        options.batch_size == 0 || options.sequences_per_worker == 0 ||
        options.duration_seconds == 0 || options.window_size_ms <= 0) {
        std::cerr << "benchmark sizes must be positive\n";
        return 1;
    }

    if (options.run_id.empty()) {
        options.run_id = std::to_string(std::chrono::duration_cast<
            std::chrono::milliseconds>(SystemClock::now().time_since_epoch()).count());
    }
    Logger logger(options.log_file);
    logger.line(
        "[mixed] address=" + options.address +
        " run_id=" + options.run_id +
        " ingest_workers=" + std::to_string(options.ingest_workers) +
        " query_workers=" + std::to_string(options.query_workers) +
        " batch_size=" + std::to_string(options.batch_size) +
        " sequences_per_worker=" +
            std::to_string(options.sequences_per_worker) +
        " duration_sec=" + std::to_string(options.duration_seconds));

    const auto channel = ::grpc::CreateChannel(
        options.address, ::grpc::InsecureChannelCredentials());
    const auto ids = makeSequenceIds(options);
    if (ids.size() < 2 || !setupConfigs(channel, ids, &logger)) {
        return 2;
    }

    Stats stats;
    const auto base_timestamp = std::chrono::duration_cast<
        std::chrono::milliseconds>(SystemClock::now().time_since_epoch()).count();
    const auto start_time = SteadyClock::now();
    std::atomic<bool> start_flag{false};
    std::vector<std::thread> ingest_workers;
    ingest_workers.reserve(options.ingest_workers);
    for (std::size_t worker = 0; worker < options.ingest_workers; ++worker) {
        const auto begin = worker * options.sequences_per_worker;
        std::vector<std::string> worker_ids(
            ids.begin() + static_cast<std::ptrdiff_t>(begin),
            ids.begin() + static_cast<std::ptrdiff_t>(
                begin + options.sequences_per_worker));
        ingest_workers.emplace_back(
            ingestMain, worker, std::cref(options), std::cref(channel),
            std::move(worker_ids), base_timestamp, start_time, &stats,
            &logger, &start_flag);
    }

    std::vector<std::thread> query_workers;
    query_workers.reserve(options.query_workers);
    for (std::size_t worker = 0; worker < options.query_workers; ++worker) {
        query_workers.emplace_back(
            queryMain, std::cref(options), std::cref(channel), std::cref(ids),
            base_timestamp, start_time, &stats, &logger, &start_flag);
    }

    std::atomic<bool> monitor_stop{false};
    std::condition_variable monitor_cv;
    std::mutex monitor_mutex;
    std::thread monitor([&] {
        std::uint64_t previous_points = 0;
        std::unique_lock lock(monitor_mutex);
        while (!monitor_stop.load(std::memory_order_acquire)) {
            if (monitor_cv.wait_for(
                    lock,
                    std::chrono::seconds(options.report_interval_seconds),
                    [&] { return monitor_stop.load(); })) {
                break;
            }
            lock.unlock();
            const auto persisted = stats.persisted_points.load();
            const auto delta = persisted - previous_points;
            previous_points = persisted;
            std::ostringstream line;
            line << std::fixed << std::setprecision(2)
                 << "[mixed] progress persisted_total=" << persisted
                 << " interval_points_per_sec=" <<
                     delta / static_cast<double>(options.report_interval_seconds)
                 << " rpc_failures=" << stats.rpc_failures.load()
                 << " storage_failed=" << stats.storage_failed_points.load()
                 << " window_failed=" << stats.window_failed_points.load();
            logger.line(line.str());
            lock.lock();
        }
    });

    const auto measured_start = SteadyClock::now();
    start_flag.store(true, std::memory_order_release);
    for (auto& worker : ingest_workers) {
        worker.join();
    }
    for (auto& worker : query_workers) {
        worker.join();
    }
    const auto measured_end = SteadyClock::now();
    monitor_stop.store(true, std::memory_order_release);
    monitor_cv.notify_all();
    monitor.join();

    const auto elapsed = std::chrono::duration<double>(
        measured_end - measured_start).count();
    logger.line(
        "[mixed] finished elapsed_sec=" + [&] {
            std::ostringstream value;
            value << std::fixed << std::setprecision(2) << elapsed;
            return value.str();
        }() +
        " submitted_points=" + std::to_string(stats.submitted_points.load()) +
        " persisted_points=" + std::to_string(stats.persisted_points.load()) +
        " persisted_points_per_sec=" + [&] {
            std::ostringstream value;
            value << std::fixed << std::setprecision(2)
                  << stats.persisted_points.load() / elapsed;
            return value.str();
        }() +
        " rpc_failures=" + std::to_string(stats.rpc_failures.load()) +
        " storage_failed_points=" +
            std::to_string(stats.storage_failed_points.load()) +
        " window_failed_points=" +
            std::to_string(stats.window_failed_points.load()));

    logCallStats("queryHistoryData", stats.history_data, &logger);
    logCallStats("queryHistoryOverview", stats.history_overview, &logger);
    logCallStats("queryWindowData", stats.window_data, &logger);
    logCallStats("alignWindowData", stats.alignment, &logger);
    logCallStats("computeBasicStatistics", stats.statistics, &logger);
    logCallStats("checkConstraints", stats.constraints, &logger);

    const fs::path output_path(options.output_file);
    std::error_code error;
    if (!output_path.parent_path().empty()) {
        fs::create_directories(output_path.parent_path(), error);
    }
    std::ofstream output(options.output_file, std::ios::out | std::ios::trunc);
    if (output.is_open()) {
        output << "metric,value\n"
               << "address," << options.address << '\n'
               << "ingest_workers," << options.ingest_workers << '\n'
               << "query_workers," << options.query_workers << '\n'
               << "batch_size," << options.batch_size << '\n'
               << "duration_seconds," << options.duration_seconds << '\n'
               << "elapsed_seconds," << elapsed << '\n'
               << "submitted_points," << stats.submitted_points.load() << '\n'
               << "persisted_points," << stats.persisted_points.load() << '\n'
               << "persisted_points_per_sec," <<
                   stats.persisted_points.load() / elapsed << '\n'
               << "rpc_failures," << stats.rpc_failures.load() << '\n'
               << "storage_failed_points," <<
                   stats.storage_failed_points.load() << '\n'
               << "window_failed_points," <<
                   stats.window_failed_points.load() << '\n';
        logger.line("[mixed] csv_result=" + options.output_file);
    } else {
        logger.line("[mixed] could not open result file: " + options.output_file);
    }

    return stats.rpc_failures.load() == 0 &&
            stats.storage_failed_points.load() == 0 &&
            stats.window_failed_points.load() == 0
        ? 0
        : 3;
}
