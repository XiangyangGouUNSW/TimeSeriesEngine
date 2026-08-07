#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
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
    std::size_t workers{4};
    std::size_t batch_size{1000};
    std::size_t sequences_per_worker{4};
    std::uint64_t duration_seconds{10};
    std::int64_t window_size_ms{3'600'000};
    std::uint64_t report_interval_seconds{2};
    std::string output_file{
        "tests/benchmarks/results/grpc_ingest_benchmark.csv"};
    std::string log_file{
        "tests/benchmarks/results/grpc_ingest_benchmark.log"};
};

class Logger {
public:
    explicit Logger(const std::string& path) {
        if (!path.empty()) {
            const fs::path file_path(path);
            std::error_code error;
            if (!file_path.parent_path().empty()) {
                fs::create_directories(file_path.parent_path(), error);
            }
            if (!error) {
                file_.open(path, std::ios::out | std::ios::trunc);
            }
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

struct Stats {
    std::atomic<std::uint64_t> submitted_points{0};
    std::atomic<std::uint64_t> completed_batches{0};
    std::atomic<std::uint64_t> persisted_points{0};
    std::atomic<std::uint64_t> storage_failed_points{0};
    std::atomic<std::uint64_t> window_updated_points{0};
    std::atomic<std::uint64_t> window_failed_points{0};
    std::atomic<std::uint64_t> resolved_points{0};
    std::atomic<std::uint64_t> resolve_failed_points{0};
    std::atomic<std::uint64_t> rpc_failures{0};

    mutable std::mutex latency_mutex;
    std::vector<double> latency_ms;

    std::mutex error_mutex;
    std::size_t error_sample_count{0};

    void recordLatency(double milliseconds) {
        std::lock_guard lock(latency_mutex);
        // A bounded sample is enough for p95/p99 and avoids making the
        // benchmark client grow without limit during long runs.
        if (latency_ms.size() < 200'000) {
            latency_ms.push_back(milliseconds);
        }
    }

    bool acceptErrorSample() {
        std::lock_guard lock(error_mutex);
        if (error_sample_count >= 5) {
            return false;
        }
        ++error_sample_count;
        return true;
    }
};

std::string operationCode(pb::OperationCode code) {
    return pb::OperationCode_Name(code);
}

bool parseUnsigned(
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
        if (value > (std::numeric_limits<std::uint64_t>::max() - digit) / 10) {
            std::cerr << option << " is too large\n";
            return false;
        }
        value = value * 10 + digit;
    }
    if (value == 0) {
        std::cerr << option << " requires a positive integer\n";
        return false;
    }
    *output = value;
    return true;
}

bool parseSigned(
    const std::string& text,
    std::int64_t* output,
    const char* option) {
    std::uint64_t unsigned_value = 0;
    if (!parseUnsigned(text, &unsigned_value, option) ||
        unsigned_value > static_cast<std::uint64_t>(
                              std::numeric_limits<std::int64_t>::max())) {
        std::cerr << option << " is out of range\n";
        return false;
    }
    *output = static_cast<std::int64_t>(unsigned_value);
    return true;
}

void printUsage(const char* program) {
    std::cout
        << "Usage: " << program << " [options]\n"
        << "  --address ADDR                 Core address (default 127.0.0.1:50051)\n"
        << "  --workers N                    concurrent client threads (default 4)\n"
        << "  --batch-size N                 points per ingestData RPC (default 1000)\n"
        << "  --sequences-per-worker N       unique sequences per worker (default 4)\n"
        << "  --duration-sec N               measured duration (default 10)\n"
        << "  --window-size-ms N             Core hot-window size (default 3600000)\n"
        << "  --report-interval-sec N        progress interval (default 2)\n"
        << "  --output PATH                  CSV result path\n"
        << "  --log-file PATH                summary log path\n"
        << "  --help                         show this help\n";
}

bool takeValue(
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
            printUsage(argv[0]);
            return false;
        }
        if (argument == "--address") {
            if (!takeValue(argc, argv, &index, "--address", &options->address)) {
                return false;
            }
        } else if (argument == "--workers" ||
                   argument == "--batch-size" ||
                   argument == "--sequences-per-worker" ||
                   argument == "--duration-sec" ||
                   argument == "--report-interval-sec") {
            if (!takeValue(argc, argv, &index, argument.c_str(), &value)) {
                return false;
            }
            std::uint64_t parsed = 0;
            if (!parseUnsigned(value, &parsed, argument.c_str())) {
                return false;
            }
            if (argument == "--workers") {
                options->workers = static_cast<std::size_t>(parsed);
            } else if (argument == "--batch-size") {
                options->batch_size = static_cast<std::size_t>(parsed);
            } else if (argument == "--sequences-per-worker") {
                options->sequences_per_worker = static_cast<std::size_t>(parsed);
            } else if (argument == "--duration-sec") {
                options->duration_seconds = parsed;
            } else {
                options->report_interval_seconds = parsed;
            }
        } else if (argument == "--window-size-ms") {
            if (!takeValue(argc, argv, &index, "--window-size-ms", &value) ||
                !parseSigned(value, &options->window_size_ms, "--window-size-ms")) {
                return false;
            }
        } else if (argument == "--output") {
            if (!takeValue(argc, argv, &index, "--output", &options->output_file)) {
                return false;
            }
        } else if (argument == "--log-file") {
            if (!takeValue(argc, argv, &index, "--log-file", &options->log_file)) {
                return false;
            }
        } else {
            std::cerr << "unknown option: " << argument << '\n';
            printUsage(argv[0]);
            return false;
        }
    }
    return true;
}

std::vector<std::string> makeSequenceIds(const Options& options) {
    std::vector<std::string> sequence_ids;
    sequence_ids.reserve(options.workers * options.sequences_per_worker);
    for (std::size_t worker = 0; worker < options.workers; ++worker) {
        for (std::size_t sequence = 0;
             sequence < options.sequences_per_worker;
             ++sequence) {
            sequence_ids.push_back(
                "grpc_bench_w" + std::to_string(worker) +
                "_s" + std::to_string(sequence));
        }
    }
    return sequence_ids;
}

bool syncInstances(
    const std::shared_ptr<::grpc::Channel>& channel,
    const std::vector<std::string>& sequence_ids,
    Logger* logger) {
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    pb::SyncInstanceConfigsRequest request;
    for (const auto& sequence_id : sequence_ids) {
        auto* item = request.add_items();
        item->set_sequence_id(sequence_id);
        item->set_data_source_id("grpc-benchmark");
        item->set_external_sequence_id(sequence_id);
        item->set_category_id("benchmark");
        item->set_data_type("double");
        item->set_series_kind(pb::SERIES_KIND_CONTINUOUS);
    }

    ::grpc::ClientContext context;
    context.set_deadline(
        std::chrono::system_clock::now() + std::chrono::seconds(10));
    pb::SyncConfigResponse response;
    const auto status = stub->syncInstanceConfigs(&context, request, &response);
    if (!status.ok()) {
        logger->line(
            "[benchmark] syncInstanceConfigs RPC failed: " +
            status.error_message());
        return false;
    }
    if (response.operation().code() != pb::OPERATION_CODE_OK) {
        logger->line(
            "[benchmark] syncInstanceConfigs failed: code=" +
            operationCode(response.operation().code()) +
            " message=" + response.operation().message());
        return false;
    }
    logger->line(
        "[benchmark] synced " + std::to_string(sequence_ids.size()) +
        " benchmark sequences");
    return true;
}

std::optional<std::uint64_t> queryPointCount(
    const std::shared_ptr<::grpc::Channel>& channel,
    const std::vector<std::string>& sequence_ids,
    Logger* logger) {
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    pb::QueryHistoryOverviewRequest request;
    for (const auto& sequence_id : sequence_ids) {
        request.add_sequence_ids(sequence_id);
    }
    ::grpc::ClientContext context;
    context.set_deadline(
        std::chrono::system_clock::now() + std::chrono::seconds(20));
    pb::QueryHistoryOverviewResponse response;
    const auto status = stub->queryHistoryOverview(&context, request, &response);
    if (!status.ok() ||
        response.operation().code() != pb::OPERATION_CODE_OK ||
        !response.has_overview()) {
        logger->line(
            "[benchmark] history overview unavailable: grpc=" +
            std::to_string(status.error_code()) +
            " code=" + operationCode(response.operation().code()) +
            " message=" + response.operation().message());
        return std::nullopt;
    }
    return response.overview().total_point_count();
}

void recordOperation(
    const pb::OperationResult& operation,
    std::atomic<std::uint64_t>* success,
    std::atomic<std::uint64_t>* failed) {
    success->fetch_add(operation.success_count(), std::memory_order_relaxed);
    failed->fetch_add(operation.failed_count(), std::memory_order_relaxed);
}

void workerMain(
    std::size_t worker_id,
    const Options& options,
    const std::shared_ptr<::grpc::Channel>& channel,
    const std::vector<std::string>& sequence_ids,
    std::int64_t base_timestamp,
    const SteadyClock::time_point& start_time,
    Stats* stats,
    Logger* logger,
    std::atomic<bool>* start_flag) {
    while (!start_flag->load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    const auto end_time = start_time +
        std::chrono::seconds(options.duration_seconds);
    const std::size_t sequence_count = sequence_ids.size();
    const std::size_t points_per_sequence =
        (options.batch_size + sequence_count - 1) / sequence_count;
    std::uint64_t batch_index = 0;

    while (SteadyClock::now() < end_time) {
        pb::IngestDataRequest request;
        request.set_window_size(options.window_size_ms);
        request.set_return_resolved_data(false);
        for (std::size_t point_index = 0;
             point_index < options.batch_size;
             ++point_index) {
            const std::size_t sequence_index = point_index % sequence_count;
            const std::size_t local_index = point_index / sequence_count;
            const auto timestamp_offset = static_cast<std::int64_t>(
                (batch_index * points_per_sequence + local_index) *
                    sequence_count + sequence_index);
            auto* point = request.add_points();
            point->set_sequence_id(sequence_ids[sequence_index]);
            point->set_time(base_timestamp + timestamp_offset);
            point->mutable_value()->set_double_value(
                static_cast<double>(worker_id) +
                static_cast<double>(batch_index * options.batch_size + point_index) *
                    0.001);
        }

        stats->submitted_points.fetch_add(
            request.points_size(), std::memory_order_relaxed);
        const auto call_start = SteadyClock::now();
        ::grpc::ClientContext context;
        context.set_wait_for_ready(true);
        context.set_deadline(
            std::chrono::system_clock::now() + std::chrono::seconds(15));
        pb::IngestDataResponse response;
        const auto status = stub->ingestData(&context, request, &response);
        const auto call_end = SteadyClock::now();
        stats->recordLatency(std::chrono::duration<double, std::milli>(
            call_end - call_start).count());
        stats->completed_batches.fetch_add(1, std::memory_order_relaxed);

        if (!status.ok()) {
            stats->rpc_failures.fetch_add(1, std::memory_order_relaxed);
            if (stats->acceptErrorSample()) {
                logger->line(
                    "[benchmark] worker=" + std::to_string(worker_id) +
                    " RPC error=" + status.error_message());
            }
            ++batch_index;
            continue;
        }

        if (response.has_resolve_result()) {
            recordOperation(
                response.resolve_result(), &stats->resolved_points,
                &stats->resolve_failed_points);
        }
        if (response.has_storage_result()) {
            recordOperation(
                response.storage_result(), &stats->persisted_points,
                &stats->storage_failed_points);
        } else {
            stats->storage_failed_points.fetch_add(
                request.points_size(), std::memory_order_relaxed);
        }
        if (response.has_window_result()) {
            recordOperation(
                response.window_result(), &stats->window_updated_points,
                &stats->window_failed_points);
        } else {
            stats->window_failed_points.fetch_add(
                request.points_size(), std::memory_order_relaxed);
        }

        if (!response.has_operation() ||
            response.operation().code() != pb::OPERATION_CODE_OK) {
            if (stats->acceptErrorSample()) {
                const auto code = response.has_operation()
                    ? operationCode(response.operation().code())
                    : "MISSING_OPERATION";
                const auto message = response.has_operation()
                    ? response.operation().message()
                    : "response has no operation result";
                logger->line(
                    "[benchmark] worker=" + std::to_string(worker_id) +
                    " ingest result code=" + code +
                    " message=" + message);
            }
        }
        ++batch_index;
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

void writeResults(
    const Options& options,
    const Stats& stats,
    double elapsed_seconds,
    std::optional<std::uint64_t> baseline_points,
    std::optional<std::uint64_t> final_points,
    Logger* logger) {
    const std::uint64_t persisted =
        stats.persisted_points.load(std::memory_order_relaxed);
    const double persisted_rate = elapsed_seconds > 0.0
        ? static_cast<double>(persisted) / elapsed_seconds
        : 0.0;

    std::vector<double> latency;
    {
        std::lock_guard lock(stats.latency_mutex);
        latency = stats.latency_ms;
    }
    const double average_latency = latency.empty()
        ? 0.0
        : std::accumulate(latency.begin(), latency.end(), 0.0) /
            static_cast<double>(latency.size());
    const double p50 = percentile(latency, 0.50);
    const double p95 = percentile(latency, 0.95);
    const double p99 = percentile(latency, 0.99);

    std::ostringstream summary;
    summary << std::fixed << std::setprecision(2)
            << "[benchmark] finished elapsed_sec=" << elapsed_seconds
            << " submitted_points=" << stats.submitted_points.load()
            << " persisted_points=" << persisted
            << " persisted_points_per_sec=" << persisted_rate
            << " rpc_failures=" << stats.rpc_failures.load()
            << " storage_failed_points=" << stats.storage_failed_points.load()
            << " window_failed_points=" << stats.window_failed_points.load();
    logger->line(summary.str());

    std::ostringstream latency_line;
    latency_line << std::fixed << std::setprecision(2)
                 << "[benchmark] rpc_latency_ms avg=" << average_latency
                 << " p50=" << p50 << " p95=" << p95 << " p99=" << p99
                 << " samples=" << latency.size();
    logger->line(latency_line.str());

    if (baseline_points && final_points) {
        const auto observed = *final_points >= *baseline_points
            ? *final_points - *baseline_points
            : 0;
        logger->line(
            "[benchmark] history_check baseline=" +
            std::to_string(*baseline_points) +
            " final=" + std::to_string(*final_points) +
            " delta=" + std::to_string(observed) +
            " persisted_ack=" + std::to_string(persisted));
    }

    const fs::path output_path(options.output_file);
    std::error_code error;
    if (!output_path.parent_path().empty()) {
        fs::create_directories(output_path.parent_path(), error);
    }
    if (error) {
        logger->line(
            "[benchmark] could not create result directory: " +
            error.message());
        return;
    }
    std::ofstream output(options.output_file, std::ios::out | std::ios::trunc);
    if (!output.is_open()) {
        logger->line(
            "[benchmark] could not open result file: " + options.output_file);
        return;
    }
    output << "metric,value\n"
           << "address," << options.address << '\n'
           << "workers," << options.workers << '\n'
           << "batch_size," << options.batch_size << '\n'
           << "sequences_per_worker," << options.sequences_per_worker << '\n'
           << "duration_seconds," << options.duration_seconds << '\n'
           << "window_size_ms," << options.window_size_ms << '\n'
           << "elapsed_seconds," << elapsed_seconds << '\n'
           << "submitted_points," << stats.submitted_points.load() << '\n'
           << "persisted_points," << persisted << '\n'
           << "persisted_points_per_sec," << persisted_rate << '\n'
           << "rpc_failures," << stats.rpc_failures.load() << '\n'
           << "storage_failed_points," << stats.storage_failed_points.load() << '\n'
           << "window_failed_points," << stats.window_failed_points.load() << '\n'
           << "latency_avg_ms," << average_latency << '\n'
           << "latency_p50_ms," << p50 << '\n'
           << "latency_p95_ms," << p95 << '\n'
           << "latency_p99_ms," << p99 << '\n';
    if (baseline_points && final_points) {
        output << "history_baseline_points," << *baseline_points << '\n'
               << "history_final_points," << *final_points << '\n'
               << "history_delta_points," << (*final_points - *baseline_points)
               << '\n';
    }
    logger->line("[benchmark] csv_result=" + options.output_file);
}

}  // namespace

int main(int argc, char* argv[]) {
    Options options;
    if (!parseOptions(argc, argv, &options)) {
        return argc > 1 && std::string(argv[1]) == "--help" ? 0 : 1;
    }
    if (options.batch_size == 0 || options.workers == 0 ||
        options.sequences_per_worker == 0 || options.window_size_ms <= 0) {
        std::cerr << "benchmark sizes must be positive\n";
        return 1;
    }

    Logger logger(options.log_file);
    logger.line(
        "[benchmark] address=" + options.address +
        " workers=" + std::to_string(options.workers) +
        " batch_size=" + std::to_string(options.batch_size) +
        " sequences_per_worker=" +
            std::to_string(options.sequences_per_worker) +
        " duration_sec=" + std::to_string(options.duration_seconds) +
        " window_size_ms=" + std::to_string(options.window_size_ms));

    const auto channel = ::grpc::CreateChannel(
        options.address, ::grpc::InsecureChannelCredentials());
    const auto sequence_ids = makeSequenceIds(options);
    if (!syncInstances(channel, sequence_ids, &logger)) {
        return 2;
    }

    const auto baseline_points = queryPointCount(channel, sequence_ids, &logger);
    if (baseline_points) {
        logger.line(
            "[benchmark] history_baseline_points=" +
            std::to_string(*baseline_points));
    }

    Stats stats;
    const auto base_timestamp = std::chrono::duration_cast<
        std::chrono::milliseconds>(SystemClock::now().time_since_epoch()).count();
    const auto start_time = SteadyClock::now();
    std::atomic<bool> start_flag{false};
    std::vector<std::thread> workers;
    workers.reserve(options.workers);
    for (std::size_t worker = 0; worker < options.workers; ++worker) {
        const auto begin = worker * options.sequences_per_worker;
        const std::vector<std::string> worker_sequences(
            sequence_ids.begin() + static_cast<std::ptrdiff_t>(begin),
            sequence_ids.begin() + static_cast<std::ptrdiff_t>(
                begin + options.sequences_per_worker));
        workers.emplace_back(
            workerMain, worker, std::cref(options), std::cref(channel),
            worker_sequences, base_timestamp, std::cref(start_time), &stats,
            &logger, &start_flag);
    }

    std::atomic<bool> monitor_stop{false};
    std::condition_variable monitor_cv;
    std::mutex monitor_mutex;
    std::thread monitor([&] {
        std::uint64_t previous_points = 0;
        std::unique_lock monitor_lock(monitor_mutex);
        while (!monitor_stop.load(std::memory_order_acquire)) {
            if (monitor_cv.wait_for(
                    monitor_lock,
                    std::chrono::seconds(options.report_interval_seconds),
                    [&] {
                        return monitor_stop.load(std::memory_order_acquire);
                    })) {
                break;
            }
            monitor_lock.unlock();
            const auto elapsed = std::chrono::duration<double>(
                SteadyClock::now() - start_time).count();
            const auto persisted =
                stats.persisted_points.load(std::memory_order_relaxed);
            const auto delta = persisted - previous_points;
            previous_points = persisted;
            std::ostringstream progress;
            progress << std::fixed << std::setprecision(2)
                     << "[benchmark] progress elapsed_sec=" << elapsed
                     << " persisted_total=" << persisted
                     << " interval_points_per_sec=" <<
                         (delta / static_cast<double>(options.report_interval_seconds))
                     << " rpc_failures=" << stats.rpc_failures.load()
                     << " storage_failed=" << stats.storage_failed_points.load()
                     << " window_failed=" << stats.window_failed_points.load();
            logger.line(progress.str());
            monitor_lock.lock();
        }
    });

    start_flag.store(true, std::memory_order_release);
    for (auto& worker : workers) {
        worker.join();
    }
    const auto measurement_end = SteadyClock::now();
    monitor_stop.store(true, std::memory_order_release);
    monitor_cv.notify_all();
    monitor.join();

    const auto elapsed_seconds = std::chrono::duration<double>(
        measurement_end - start_time).count();
    const auto final_points = queryPointCount(channel, sequence_ids, &logger);
    writeResults(
        options, stats, elapsed_seconds, baseline_points, final_points, &logger);
    return stats.rpc_failures.load() == 0 &&
            stats.storage_failed_points.load() == 0 &&
            stats.window_failed_points.load() == 0
        ? 0
        : 3;
}
