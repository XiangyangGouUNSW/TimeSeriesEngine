#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "timeseries_core.grpc.pb.h"

namespace {

namespace pb = ::sfkg::timeseries::core::v1;
using Clock = std::chrono::steady_clock;

struct Options {
    std::string address{"127.0.0.1:50051"};
    std::string project_id{"grpc-ordered-load-test"};
    std::string prefix;
    std::size_t sequence_count{32};
    std::size_t rows{50'000};
    std::size_t batch_rows{1'000};
    std::size_t workers{1};
    std::size_t derived_count{4};
    std::size_t constraint_count{4};
    std::int64_t window_size_ms{3'600'000};
    std::int64_t sample_interval_ms{1'000};
    std::int64_t base_time_ms{0};
    std::uint64_t rpc_timeout_sec{120};
};

struct Stats {
    std::atomic<std::uint64_t> submitted_points{0};
    std::atomic<std::uint64_t> accepted_points{0};
    std::atomic<std::uint64_t> failed_points{0};
    std::atomic<std::uint64_t> completed_batches{0};
    std::atomic<std::uint64_t> rpc_failures{0};
    std::atomic<std::uint64_t> non_ok_results{0};
    std::mutex latency_mutex;
    std::vector<double> latencies_ms;
    std::mutex error_mutex;
    std::string first_error;

    void error(const std::string& message) {
        std::lock_guard lock(error_mutex);
        if (first_error.empty()) {
            first_error = message;
        }
    }

    void latency(double milliseconds) {
        std::lock_guard lock(latency_mutex);
        latencies_ms.push_back(milliseconds);
    }
};

class BatchBarrier {
public:
    explicit BatchBarrier(std::size_t parties) : parties_(parties) {}

    void wait() {
        std::unique_lock lock(mutex_);
        const auto generation = generation_;
        if (++arrived_ == parties_) {
            arrived_ = 0;
            ++generation_;
            condition_.notify_all();
            return;
        }
        condition_.wait(lock, [&] { return generation_ != generation; });
    }

private:
    const std::size_t parties_;
    std::size_t arrived_{0};
    std::size_t generation_{0};
    std::mutex mutex_;
    std::condition_variable condition_;
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

bool parseNonNegative(
    const std::string& text,
    std::uint64_t* output,
    const char* option) {
    if (text.empty()) {
        std::cerr << option << " requires a non-negative integer\n";
        return false;
    }
    std::uint64_t value = 0;
    for (const char ch : text) {
        if (ch < '0' || ch > '9') {
            std::cerr << option << " requires a non-negative integer\n";
            return false;
        }
        const auto digit = static_cast<std::uint64_t>(ch - '0');
        if (value > (std::numeric_limits<std::uint64_t>::max() - digit) / 10) {
            std::cerr << option << " is too large\n";
            return false;
        }
        value = value * 10 + digit;
    }
    *output = value;
    return true;
}

bool parseSigned(
    const std::string& text,
    std::int64_t* output,
    const char* option) {
    bool negative = false;
    std::string digits = text;
    if (!digits.empty() && digits.front() == '-') {
        negative = true;
        digits.erase(digits.begin());
    }
    std::uint64_t value = 0;
    if (!parseUnsigned(digits, &value, option)) {
        return false;
    }
    if ((!negative && value > static_cast<std::uint64_t>(
                              std::numeric_limits<std::int64_t>::max())) ||
        (negative && value > static_cast<std::uint64_t>(
                              std::numeric_limits<std::int64_t>::max()) + 1)) {
        std::cerr << option << " is out of range\n";
        return false;
    }
    if (negative) {
        if (value == static_cast<std::uint64_t>(
                         std::numeric_limits<std::int64_t>::max()) + 1) {
            *output = std::numeric_limits<std::int64_t>::min();
        } else {
            *output = -static_cast<std::int64_t>(value);
        }
    } else {
        *output = static_cast<std::int64_t>(value);
    }
    return true;
}

bool takeValue(
    int argc,
    char* argv[],
    int* index,
    const std::string& option,
    std::string* value) {
    if (*index + 1 >= argc) {
        std::cerr << option << " requires a value\n";
        return false;
    }
    *value = argv[++(*index)];
    return true;
}

void printUsage(const char* program) {
    std::cout
        << "Usage: " << program << " [options]\n"
        << "  --address ADDR             Core address (default 127.0.0.1:50051)\n"
        << "  --project-id TEXT          Project scope (default grpc-ordered-load-test)\n"
        << "  --prefix TEXT              unique ID prefix\n"
        << "  --sequences N              raw sequence count (default 32)\n"
        << "  --rows N                   measured timestamps (default 50000)\n"
        << "  --batch-rows N             timestamps per RPC lane (default 1000)\n"
        << "  --workers N                ordered sequence lanes (default 1)\n"
        << "  --derived N                derived configs (default 4)\n"
        << "  --constraints N            constraint configs (default 4)\n"
        << "  --window-ms N              Core hot-window size (default 3600000)\n"
        << "  --sample-ms N              logical sample interval (default 1000)\n"
        << "  --base-time-ms N           first timestamp (default: current time)\n"
        << "  --rpc-timeout-sec N        per-RPC deadline (default 120)\n"
        << "  --help                     show this help\n";
}

bool parseOptions(int argc, char* argv[], Options* options) {
    if (const char* configured = std::getenv("SFKG_TIMESERIES_CORE_ADDRESS")) {
        options->address = configured;
    }
    for (int index = 1; index < argc; ++index) {
        const std::string argument = argv[index];
        std::string value;
        if (argument == "--help") {
            printUsage(argv[0]);
            return false;
        }
        if (argument == "--address" || argument == "--project-id" ||
            argument == "--prefix") {
            if (!takeValue(argc, argv, &index, argument, &value)) {
                return false;
            }
            if (argument == "--address") {
                options->address = value;
            } else if (argument == "--project-id") {
                options->project_id = value;
            } else {
                options->prefix = value;
            }
            continue;
        }
        if (argument == "--sequences" || argument == "--rows" ||
            argument == "--batch-rows" || argument == "--workers" ||
            argument == "--derived" || argument == "--constraints" ||
            argument == "--rpc-timeout-sec") {
            if (!takeValue(argc, argv, &index, argument, &value)) {
                return false;
            }
            std::uint64_t parsed = 0;
            const bool allows_zero = argument == "--derived" ||
                argument == "--constraints";
            const bool valid = allows_zero
                ? parseNonNegative(value, &parsed, argument.c_str())
                : parseUnsigned(value, &parsed, argument.c_str());
            if (!valid) {
                return false;
            }
            if (argument == "--sequences") {
                options->sequence_count = static_cast<std::size_t>(parsed);
            } else if (argument == "--rows") {
                options->rows = static_cast<std::size_t>(parsed);
            } else if (argument == "--batch-rows") {
                options->batch_rows = static_cast<std::size_t>(parsed);
            } else if (argument == "--workers") {
                options->workers = static_cast<std::size_t>(parsed);
            } else if (argument == "--derived") {
                options->derived_count = static_cast<std::size_t>(parsed);
            } else if (argument == "--constraints") {
                options->constraint_count = static_cast<std::size_t>(parsed);
            } else {
                options->rpc_timeout_sec = parsed;
            }
            continue;
        }
        if (argument == "--window-ms" || argument == "--sample-ms" ||
            argument == "--base-time-ms") {
            if (!takeValue(argc, argv, &index, argument, &value)) {
                return false;
            }
            auto* output = argument == "--window-ms"
                ? &options->window_size_ms
                : argument == "--sample-ms"
                    ? &options->sample_interval_ms
                    : &options->base_time_ms;
            if (!parseSigned(value, output, argument.c_str())) {
                return false;
            }
            continue;
        }
        std::cerr << "unknown option: " << argument << '\n';
        printUsage(argv[0]);
        return false;
    }
    return true;
}

std::string makePrefix(const Options& options) {
    if (!options.prefix.empty()) {
        return options.prefix;
    }
    const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return "grpc-ordered-" + std::to_string(now);
}

std::vector<std::string> makeSequenceIds(
    const std::string& prefix,
    std::size_t count) {
    std::vector<std::string> ids;
    ids.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
        ids.push_back(prefix + "-s" + std::to_string(index));
    }
    return ids;
}

bool successful(const pb::OperationResult& operation) {
    return operation.code() == pb::OPERATION_CODE_OK ||
        operation.code() == pb::OPERATION_CODE_PARTIAL_SUCCESS;
}

bool syncInstances(
    const std::shared_ptr<::grpc::Channel>& channel,
    const std::string& project_id,
    const std::string& prefix,
    const std::vector<std::string>& sequence_ids) {
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    pb::SyncInstanceConfigsRequest request;
    request.set_project_id(project_id);
    for (const auto& sequence_id : sequence_ids) {
        auto* item = request.add_items();
        item->set_project_id(project_id);
        item->set_sequence_id(sequence_id);
        item->set_data_source_id(prefix + "-source");
        item->set_external_sequence_id(sequence_id);
        item->set_category_id("grpc-ordered-load-test");
        item->set_data_type("double");
        item->set_series_kind(pb::SERIES_KIND_CONTINUOUS);
    }
    pb::SyncConfigResponse response;
    ::grpc::ClientContext context;
    context.set_wait_for_ready(true);
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(30));
    const auto status = stub->syncInstanceConfigs(&context, request, &response);
    if (!status.ok() || !response.has_operation() ||
        response.operation().code() != pb::OPERATION_CODE_OK) {
        std::cerr << "syncInstanceConfigs failed grpc=" << status.error_code()
                  << " code=" << operationCode(response.operation().code())
                  << " message=" << response.operation().message() << '\n';
        return false;
    }
    return true;
}

bool syncWindow(
    const std::shared_ptr<::grpc::Channel>& channel,
    const std::string& project_id,
    std::int64_t window_size_ms) {
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    pb::SyncWindowConfigRequest request;
    request.set_project_id(project_id);
    request.mutable_config()->set_project_id(project_id);
    request.mutable_config()->set_window_size(window_size_ms);
    pb::SyncConfigResponse response;
    ::grpc::ClientContext context;
    context.set_wait_for_ready(true);
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(30));
    const auto status = stub->syncWindowConfig(&context, request, &response);
    if (!status.ok() || !response.has_operation() ||
        response.operation().code() != pb::OPERATION_CODE_OK) {
        std::cerr << "syncWindowConfig failed grpc=" << status.error_code()
                  << " code=" << operationCode(response.operation().code())
                  << " message=" << response.operation().message() << '\n';
        return false;
    }
    return true;
}

bool syncConstraints(
    const std::shared_ptr<::grpc::Channel>& channel,
    const std::string& project_id,
    const std::string& prefix,
    const std::vector<std::string>& sequence_ids,
    std::size_t count) {
    if (count == 0) {
        return true;
    }
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    pb::SyncConstraintsRequest request;
    request.set_project_id(project_id);
    for (std::size_t constraint = 0; constraint < count; ++constraint) {
        auto* item = request.add_items();
        item->set_project_id(project_id);
        item->set_enabled(true);
        auto* rule = item->mutable_rule();
        rule->set_project_id(project_id);
        rule->set_constraint_id(
            prefix + "-constraint-" + std::to_string(constraint));
        rule->set_lower_bound(-1.0e9);
        rule->set_upper_bound(1.0e9);
        // Alternate single-sequence and multi-sequence rules so this load
        // test exercises both constraint paths. Multi-sequence rules use
        // three or four terms to avoid making every rule identical.
        const bool single_sequence = constraint % 2 == 0;
        const std::size_t term_count = single_sequence
            ? 1
            : 3 + (constraint % 2);
        for (std::size_t term_index = 0; term_index < term_count;
             ++term_index) {
            const auto sequence_index =
                (constraint * 4 + term_index) % sequence_ids.size();
            const auto variable = "v" + std::to_string(term_index);
            (*rule->mutable_variable_mapping())[variable] =
                sequence_ids[sequence_index];
            auto* term = rule->add_terms();
            term->set_variable(variable);
            term->set_coefficient(
                single_sequence
                    ? 1.0 + static_cast<double>(constraint) * 0.1
                    : term_index == 0 ? 1.0
                    : term_index == 1 ? -0.5
                                      : 0.25);
            term->set_sample_offset(0);
        }
    }
    pb::SyncConfigResponse response;
    ::grpc::ClientContext context;
    context.set_wait_for_ready(true);
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(30));
    const auto status = stub->syncConstraints(&context, request, &response);
    if (!status.ok() || !response.has_operation() ||
        response.operation().code() != pb::OPERATION_CODE_OK) {
        std::cerr << "syncConstraints failed grpc=" << status.error_code()
                  << " code=" << operationCode(response.operation().code())
                  << " message=" << response.operation().message() << '\n';
        return false;
    }
    return true;
}

bool syncDerived(
    const std::shared_ptr<::grpc::Channel>& channel,
    const std::string& project_id,
    const std::string& prefix,
    const std::vector<std::string>& sequence_ids,
    std::size_t count) {
    if (count == 0) {
        return true;
    }
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    pb::SyncDerivedSeriesConfigsRequest request;
    request.set_project_id(project_id);
    for (std::size_t derived = 0; derived < count; ++derived) {
        auto* item = request.add_items();
        item->set_project_id(project_id);
        item->set_derived_sequence_id(
            prefix + "-derived-" + std::to_string(derived));
        item->set_enabled(true);
        auto* formula = item->mutable_linear_combination();
        formula->set_bias(static_cast<double>(derived));
        for (std::size_t term_index = 0; term_index < 4; ++term_index) {
            auto* term = formula->add_terms();
            term->set_sequence_id(sequence_ids[
                (derived * 5 + term_index) % sequence_ids.size()]);
            term->set_coefficient(
                term_index == 0 ? 1.0
                : term_index == 1 ? 0.5
                : term_index == 2 ? -0.25
                                   : 0.125);
        }
    }
    pb::SyncConfigResponse response;
    ::grpc::ClientContext context;
    context.set_wait_for_ready(true);
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(30));
    const auto status = stub->syncDerivedSeriesConfigs(
        &context, request, &response);
    if (!status.ok() || !response.has_operation() ||
        response.operation().code() != pb::OPERATION_CODE_OK) {
        std::cerr << "syncDerivedSeriesConfigs failed grpc="
                  << status.error_code()
                  << " code=" << operationCode(response.operation().code())
                  << " message=" << response.operation().message() << '\n';
        return false;
    }
    return true;
}

double valueFor(std::size_t row, std::size_t sequence) {
    const auto phase = static_cast<double>(row * 13 + sequence * 7) * 0.01;
    return 10.0 + static_cast<double>(sequence) * 0.1 + 2.0 * std::sin(phase);
}

void workerMain(
    std::size_t worker_id,
    const Options& options,
    const std::string& project_id,
    const std::string& prefix,
    const std::vector<std::string>& sequence_ids,
    BatchBarrier* barrier,
    const std::shared_ptr<::grpc::Channel>& channel,
    std::int64_t base_time_ms,
    Stats* stats) {
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    for (std::size_t batch_start = 0; batch_start < options.rows;
         batch_start += options.batch_rows) {
        const auto batch_end = std::min(options.rows,
                                        batch_start + options.batch_rows);
        pb::IngestDataRequest request;
        request.set_project_id(project_id);
        request.set_return_resolved_data(false);
        for (std::size_t row = batch_start; row < batch_end; ++row) {
            for (std::size_t sequence = worker_id;
                 sequence < sequence_ids.size(); sequence += options.workers) {
                auto* point = request.add_points();
                point->set_project_id(project_id);
                point->set_sequence_id(sequence_ids[sequence]);
                point->set_data_source_id(prefix + "-source");
                point->set_external_sequence_id(sequence_ids[sequence]);
                point->set_time(base_time_ms + static_cast<std::int64_t>(row) *
                                options.sample_interval_ms);
                point->mutable_value()->set_double_value(
                    valueFor(row, sequence));
            }
        }

        stats->submitted_points.fetch_add(
            static_cast<std::uint64_t>(request.points_size()),
            std::memory_order_relaxed);
        const auto started = Clock::now();
        ::grpc::ClientContext context;
        context.set_wait_for_ready(true);
        context.set_deadline(std::chrono::system_clock::now() +
                             std::chrono::seconds(options.rpc_timeout_sec));
        pb::IngestDataResponse response;
        const auto status = stub->ingestData(&context, request, &response);
        const auto elapsed = std::chrono::duration<double, std::milli>(
            Clock::now() - started).count();
        stats->latency(elapsed);
        stats->completed_batches.fetch_add(1, std::memory_order_relaxed);

        if (!status.ok()) {
            stats->rpc_failures.fetch_add(1, std::memory_order_relaxed);
            stats->failed_points.fetch_add(
                static_cast<std::uint64_t>(request.points_size()),
                std::memory_order_relaxed);
            stats->error(
                "worker=" + std::to_string(worker_id) +
                " batch_start=" + std::to_string(batch_start) +
                " grpc=" + status.error_message());
        } else if (!response.has_operation()) {
            stats->non_ok_results.fetch_add(1, std::memory_order_relaxed);
            stats->failed_points.fetch_add(
                static_cast<std::uint64_t>(request.points_size()),
                std::memory_order_relaxed);
            stats->error("response has no operation result");
        } else {
            const auto& operation = response.operation();
            stats->accepted_points.fetch_add(
                operation.success_count(), std::memory_order_relaxed);
            stats->failed_points.fetch_add(
                operation.failed_count(), std::memory_order_relaxed);
            if (!successful(operation) ||
                (options.derived_count > 0 && response.has_derived_result() &&
                 !successful(response.derived_result()))) {
                stats->non_ok_results.fetch_add(1, std::memory_order_relaxed);
                stats->error(
                    "worker=" + std::to_string(worker_id) +
                    " batch_start=" + std::to_string(batch_start) +
                    " operation=" + operationCode(operation.code()) +
                    " message=" + operation.message());
            }
        }

        // This is the ordering guarantee: no worker can submit the next
        // logical time block until every sequence lane completed this block.
        barrier->wait();
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

int run(const Options& options) {
    const auto prefix = makePrefix(options);
    const auto sequence_ids = makeSequenceIds(prefix, options.sequence_count);
    const auto channel = ::grpc::CreateChannel(
        options.address, ::grpc::InsecureChannelCredentials());
    if (!channel->WaitForConnected(
            std::chrono::system_clock::now() + std::chrono::seconds(10))) {
        std::cerr << "Core is not reachable at " << options.address << '\n';
        return 2;
    }
    if (options.project_id.empty()) {
        std::cerr << "--project-id must not be empty\n";
        return 2;
    }
    if (!syncInstances(channel, options.project_id, prefix, sequence_ids) ||
        !syncWindow(channel, options.project_id, options.window_size_ms) ||
        !syncConstraints(
            channel, options.project_id, prefix, sequence_ids,
            options.constraint_count) ||
        !syncDerived(
            channel, options.project_id, prefix, sequence_ids,
            options.derived_count)) {
        return 2;
    }

    const auto total_points = static_cast<std::uint64_t>(options.rows) *
        static_cast<std::uint64_t>(options.sequence_count);
    const auto batch_count =
        (options.rows + options.batch_rows - 1) / options.batch_rows;
    std::cout << "grpc ordered load test\n"
              << "  address=" << options.address << '\n'
              << "  project_id=" << options.project_id << '\n'
              << "  prefix=" << prefix << '\n'
              << "  sequences=" << options.sequence_count << '\n'
              << "  rows=" << options.rows << '\n'
              << "  points=" << total_points << '\n'
              << "  batches=" << batch_count << '\n'
              << "  batch_rows=" << options.batch_rows << '\n'
              << "  workers=" << options.workers << '\n'
              << "  window_ms=" << options.window_size_ms << '\n'
              << "  sample_ms=" << options.sample_interval_ms << '\n'
              << "  derived=" << options.derived_count << '\n'
              << "  constraints=" << options.constraint_count << '\n'
              << "  cross_sequence_timestamp_skew_ms=0\n";

    Stats stats;
    BatchBarrier barrier(options.workers);
    const auto base_time = options.base_time_ms != 0
        ? options.base_time_ms
        : std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch()).count();
    const auto started = Clock::now();
    std::vector<std::thread> workers;
    workers.reserve(options.workers);
    for (std::size_t worker = 0; worker < options.workers; ++worker) {
        workers.emplace_back(
            workerMain, worker, std::cref(options), std::cref(options.project_id),
            std::cref(prefix),
            std::cref(sequence_ids), &barrier, std::cref(channel), base_time,
            &stats);
    }
    for (auto& worker : workers) {
        worker.join();
    }
    const auto elapsed = std::chrono::duration<double>(
        Clock::now() - started).count();

    std::vector<double> latencies;
    {
        std::lock_guard lock(stats.latency_mutex);
        latencies = stats.latencies_ms;
    }
    const auto submitted = stats.submitted_points.load();
    const auto accepted = stats.accepted_points.load();
    const auto failed = stats.failed_points.load();
    const bool exact = submitted == total_points && accepted == total_points &&
        failed == 0 && stats.rpc_failures.load() == 0 &&
        stats.non_ok_results.load() == 0;
    std::cout << std::fixed << std::setprecision(2)
              << "result elapsed_sec=" << elapsed
              << " submitted_points=" << submitted
              << " accepted_points=" << accepted
              << " failed_points=" << failed
              << " completed_batches=" << stats.completed_batches.load()
              << " rpc_failures=" << stats.rpc_failures.load()
              << " non_ok_results=" << stats.non_ok_results.load()
              << " points_per_sec="
              << (elapsed > 0.0 ? static_cast<double>(accepted) / elapsed : 0.0)
              << " rows_per_sec="
              << (elapsed > 0.0 ? static_cast<double>(options.rows) / elapsed : 0.0)
              << " rpc_avg_ms="
              << (latencies.empty() ? 0.0
                  : std::accumulate(latencies.begin(), latencies.end(), 0.0) /
                      static_cast<double>(latencies.size()))
              << " rpc_p50_ms=" << percentile(latencies, 0.50)
              << " rpc_p95_ms=" << percentile(latencies, 0.95)
              << "\n";
    if (!stats.first_error.empty()) {
        std::cerr << "first_error=" << stats.first_error << '\n';
    }
    return exact ? 0 : 3;
}

}  // namespace

int main(int argc, char* argv[]) {
    Options options;
    if (!parseOptions(argc, argv, &options)) {
        return argc > 1 && std::string(argv[1]) == "--help" ? 0 : 1;
    }
    if (options.address.empty() || options.sequence_count == 0 ||
        options.rows == 0 || options.batch_rows == 0 || options.workers == 0 ||
        options.workers > options.sequence_count || options.window_size_ms <= 0 ||
        options.sample_interval_ms <= 0 || options.rpc_timeout_sec == 0) {
        std::cerr << "invalid load-test options; workers must be <= sequences\n";
        return 1;
    }
    return run(options);
}
