#include <algorithm>
#include <chrono>
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
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "timeseries_core.grpc.pb.h"

namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;
using SystemClock = std::chrono::system_clock;

namespace {

namespace pb = ::sfkg::timeseries::core::v1;

struct Options {
    std::string address{"127.0.0.1:50051"};
    std::size_t batch_size{1000};
    std::size_t batches{10};
    std::size_t query_repetitions{10};
    std::int64_t window_size_ms{3'600'000};
    std::string output_file{
        "tests/benchmarks/results/grpc_stage_benchmark.csv"};
    std::string log_file{
        "tests/benchmarks/results/grpc_stage_benchmark.log"};
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

    void line(const std::string& value) {
        std::lock_guard lock(mutex_);
        std::cout << value << '\n';
        if (file_.is_open()) {
            file_ << value << '\n';
            file_.flush();
        }
    }

private:
    std::mutex mutex_;
    std::ofstream file_;
};

struct StageStats {
    std::uint64_t calls{0};
    std::uint64_t successes{0};
    std::uint64_t failures{0};
    std::vector<double> latency_ms;
};

struct PointSpec {
    std::string sequence_id;
    std::int64_t time{0};
    double value{0.0};
};

struct CallResult {
    ::grpc::Status status;
    pb::OperationCode code{pb::OPERATION_CODE_INTERNAL_ERROR};
    std::string message;

    bool successful() const {
        return status.ok() &&
            (code == pb::OPERATION_CODE_OK ||
             code == pb::OPERATION_CODE_PARTIAL_SUCCESS);
    }
};

std::string operationCode(pb::OperationCode code) {
    return pb::OperationCode_Name(code);
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
        value = value * 10 + static_cast<std::uint64_t>(ch - '0');
    }
    if (value == 0) {
        std::cerr << option << " requires a positive integer\n";
        return false;
    }
    *output = value;
    return true;
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

void usage(const char* program) {
    std::cout
        << "Usage: " << program << " [options]\n"
        << "  --address ADDR          Core address\n"
        << "  --batch-size N          points per write RPC (default 1000)\n"
        << "  --batches N             batches per write stage (default 10)\n"
        << "  --query-repetitions N   history query repetitions (default 10)\n"
        << "  --window-size-ms N      hot window size (default 3600000)\n"
        << "  --output PATH           CSV result path\n"
        << "  --log-file PATH         summary log path\n"
        << "  --help                  show this help\n";
}

bool parseOptions(int argc, char* argv[], Options* options) {
    for (int index = 1; index < argc; ++index) {
        const std::string argument = argv[index];
        if (argument == "--help") {
            usage(argv[0]);
            return false;
        }
        std::string value;
        if (argument == "--address" || argument == "--output" ||
            argument == "--log-file") {
            if (!nextValue(argc, argv, &index, argument.c_str(), &value)) {
                return false;
            }
            if (argument == "--address") {
                options->address = value;
            } else if (argument == "--output") {
                options->output_file = value;
            } else {
                options->log_file = value;
            }
            continue;
        }
        if (!nextValue(argc, argv, &index, argument.c_str(), &value)) {
            return false;
        }
        std::uint64_t parsed = 0;
        if (!parsePositive(value, &parsed, argument.c_str())) {
            return false;
        }
        if (argument == "--batch-size") {
            options->batch_size = static_cast<std::size_t>(parsed);
        } else if (argument == "--batches") {
            options->batches = static_cast<std::size_t>(parsed);
        } else if (argument == "--query-repetitions") {
            options->query_repetitions = static_cast<std::size_t>(parsed);
        } else if (argument == "--window-size-ms") {
            options->window_size_ms = static_cast<std::int64_t>(parsed);
        } else {
            std::cerr << "unknown option: " << argument << '\n';
            usage(argv[0]);
            return false;
        }
    }
    return true;
}

std::vector<std::string> makeIds(const std::string& stage, std::int64_t run_id) {
    std::vector<std::string> ids;
    for (int index = 0; index < 4; ++index) {
        ids.push_back(
            "grpc_stage_" + stage + "_r" + std::to_string(run_id) +
            "_s" + std::to_string(index));
    }
    return ids;
}

std::vector<PointSpec> makePoints(
    const std::vector<std::string>& ids,
    std::size_t batch_index,
    std::size_t batch_size,
    std::int64_t base_time) {
    std::vector<PointSpec> points;
    points.reserve(batch_size);
    for (std::size_t index = 0; index < batch_size; ++index) {
        points.push_back({
            ids[index % ids.size()],
            base_time + static_cast<std::int64_t>(
                batch_index * batch_size + index),
            static_cast<double>(batch_index * batch_size + index) * 0.001});
    }
    return points;
}

void fillIngest(
    pb::IngestDataRequest* request,
    const std::vector<PointSpec>& points) {
    for (const auto& item : points) {
        auto* point = request->add_points();
        point->set_sequence_id(item.sequence_id);
        point->set_data_source_id("grpc-stage-benchmark");
        point->set_external_sequence_id(item.sequence_id);
        point->set_time(item.time);
        point->mutable_value()->set_double_value(item.value);
    }
}

void fillRaw(
    pb::TimeseriesBatch* batch,
    const std::vector<PointSpec>& points) {
    for (const auto& item : points) {
        auto* point = batch->add_points();
        point->set_sequence_id(item.sequence_id);
        point->set_time(item.time);
        point->mutable_value()->set_double_value(item.value);
    }
}

void record(
    const char* name,
    const CallResult& result,
    double elapsed_ms,
    StageStats* stats,
    Logger* logger) {
    ++stats->calls;
    if (result.successful()) {
        ++stats->successes;
    } else {
        ++stats->failures;
        logger->line(
            std::string("[stage] ") + name +
            " failed grpc=" + std::to_string(result.status.error_code()) +
            " operation=" + operationCode(result.code) +
            " message=" + (result.message.empty()
                ? result.status.error_message()
                : result.message));
    }
    stats->latency_ms.push_back(elapsed_ms);
}

template <typename Invoke>
void measure(
    const char* name,
    StageStats* stats,
    Logger* logger,
    Invoke&& invoke) {
    const auto started = Clock::now();
    const CallResult result = invoke();
    const auto elapsed = std::chrono::duration<double, std::milli>(
        Clock::now() - started).count();
    record(name, result, elapsed, stats, logger);
}

bool syncInstances(
    const std::shared_ptr<::grpc::Channel>& channel,
    const std::vector<std::string>& ids,
    Logger* logger) {
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    pb::SyncInstanceConfigsRequest request;
    for (const auto& id : ids) {
        auto* item = request.add_items();
        item->set_sequence_id(id);
        item->set_data_source_id("grpc-stage-benchmark");
        item->set_external_sequence_id(id);
        item->set_category_id("benchmark");
        item->set_data_type("double");
        item->set_series_kind(pb::SERIES_KIND_CONTINUOUS);
    }
    ::grpc::ClientContext context;
    pb::SyncConfigResponse response;
    const auto status = stub->syncInstanceConfigs(&context, request, &response);
    logger->line(
        "[stage] syncInstanceConfigs grpc=" +
        std::to_string(status.error_code()) +
        " operation=" + operationCode(response.operation().code()) +
        " sequences=" + std::to_string(ids.size()));
    return status.ok() && response.operation().code() == pb::OPERATION_CODE_OK;
}

bool syncWindowConfig(
    const std::shared_ptr<::grpc::Channel>& channel,
    std::int64_t window_size,
    Logger* logger) {
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    pb::SyncWindowConfigRequest request;
    request.mutable_config()->set_window_size(window_size);
    ::grpc::ClientContext context;
    pb::SyncConfigResponse response;
    const auto status = stub->syncWindowConfig(&context, request, &response);
    logger->line(
        "[stage] syncWindowConfig grpc=" +
        std::to_string(status.error_code()) +
        " operation=" + operationCode(response.operation().code()) +
        " window_size_ms=" + std::to_string(window_size));
    return status.ok() && response.operation().code() == pb::OPERATION_CODE_OK;
}

CallResult makeResult(
    const ::grpc::Status& status,
    const pb::OperationResult& operation) {
    return {status, operation.code(), operation.message()};
}

void runResolve(
    const std::shared_ptr<::grpc::Channel>& channel,
    const std::vector<std::string>& ids,
    const Options& options,
    std::int64_t base_time,
    StageStats* stats,
    Logger* logger) {
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    for (std::size_t batch = 0; batch < options.batches; ++batch) {
        const auto points = makePoints(ids, batch, options.batch_size, base_time);
        pb::IngestRequest request;
        for (const auto& item : points) {
            auto* point = request.add_points();
            point->set_sequence_id(item.sequence_id);
            point->set_data_source_id("grpc-stage-benchmark");
            point->set_external_sequence_id(item.sequence_id);
            point->set_time(item.time);
            point->mutable_value()->set_double_value(item.value);
        }
        measure("ingestAndResolveData", stats, logger, [&] {
            ::grpc::ClientContext context;
            pb::IngestResponse response;
            const auto status = stub->ingestAndResolveData(
                &context, request, &response);
            return makeResult(status, response.operation());
        });
    }
}

void runRawWrite(
    const std::shared_ptr<::grpc::Channel>& channel,
    const std::vector<std::string>& ids,
    const Options& options,
    std::int64_t base_time,
    StageStats* stats,
    Logger* logger) {
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    for (std::size_t batch = 0; batch < options.batches; ++batch) {
        const auto points = makePoints(ids, batch, options.batch_size, base_time);
        pb::WriteRawDataRequest request;
        fillRaw(request.mutable_data(), points);
        measure("writeRawData", stats, logger, [&] {
            ::grpc::ClientContext context;
            pb::WriteRawDataResponse response;
            const auto status = stub->writeRawData(
                &context, request, &response);
            return makeResult(status, response.operation());
        });
    }
}

void runWindowBuild(
    const std::shared_ptr<::grpc::Channel>& channel,
    const std::vector<std::string>& ids,
    const Options& options,
    std::int64_t base_time,
    StageStats* stats,
    Logger* logger) {
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    for (std::size_t batch = 0; batch < options.batches; ++batch) {
        const auto points = makePoints(ids, batch, options.batch_size, base_time);
        pb::BuildTimeWindowRequest request;
        fillRaw(request.mutable_data(), points);
        request.set_window_size(options.window_size_ms);
        measure("buildTimeWindow", stats, logger, [&] {
            ::grpc::ClientContext context;
            pb::BuildTimeWindowResponse response;
            const auto status = stub->buildTimeWindow(
                &context, request, &response);
            return makeResult(status, response.operation());
        });
    }
}

void runFullIngest(
    const std::shared_ptr<::grpc::Channel>& channel,
    const std::vector<std::string>& ids,
    const Options& options,
    std::int64_t base_time,
    StageStats* stats,
    Logger* logger) {
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    for (std::size_t batch = 0; batch < options.batches; ++batch) {
        const auto points = makePoints(ids, batch, options.batch_size, base_time);
        pb::IngestDataRequest request;
        fillIngest(&request, points);
        measure("ingestData", stats, logger, [&] {
            ::grpc::ClientContext context;
            pb::IngestDataResponse response;
            const auto status = stub->ingestData(
                &context, request, &response);
            return makeResult(status, response.operation());
        });
    }
}

void runHistoryQueries(
    const std::shared_ptr<::grpc::Channel>& channel,
    const std::vector<std::string>& ids,
    const Options& options,
    std::int64_t base_time,
    StageStats* history_stats,
    StageStats* overview_stats,
    Logger* logger) {
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    const auto end_time = base_time + static_cast<std::int64_t>(
        options.batches * options.batch_size + 1);
    for (std::size_t repetition = 0;
         repetition < options.query_repetitions;
         ++repetition) {
        pb::QueryHistoryDataRequest data_request;
        for (const auto& id : ids) {
            data_request.add_sequence_ids(id);
        }
        data_request.set_start_time(base_time);
        data_request.set_end_time(end_time);
        measure("queryHistoryData", history_stats, logger, [&] {
            ::grpc::ClientContext context;
            pb::QueryHistoryDataResponse response;
            const auto status = stub->queryHistoryData(
                &context, data_request, &response);
            return makeResult(status, response.operation());
        });

        pb::QueryHistoryOverviewRequest overview_request;
        for (const auto& id : ids) {
            overview_request.add_sequence_ids(id);
        }
        overview_request.set_start_time(base_time);
        overview_request.set_end_time(end_time);
        measure("queryHistoryOverview", overview_stats, logger, [&] {
            ::grpc::ClientContext context;
            pb::QueryHistoryOverviewResponse response;
            const auto status = stub->queryHistoryOverview(
                &context, overview_request, &response);
            return makeResult(status, response.operation());
        });
    }
}

double percentile(std::vector<double> values, double fraction) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    return values[static_cast<std::size_t>(
        fraction * static_cast<double>(values.size() - 1))];
}

void logStats(const char* name, const StageStats& stats, Logger* logger) {
    const double average = stats.latency_ms.empty()
        ? 0.0
        : std::accumulate(
              stats.latency_ms.begin(), stats.latency_ms.end(), 0.0) /
            static_cast<double>(stats.latency_ms.size());
    std::ostringstream line;
    line << std::fixed << std::setprecision(2)
         << "[stage] rpc=" << name
         << " calls=" << stats.calls
         << " successes=" << stats.successes
         << " failures=" << stats.failures
         << " avg_ms=" << average
         << " p50_ms=" << percentile(stats.latency_ms, 0.50)
         << " p95_ms=" << percentile(stats.latency_ms, 0.95)
         << " p99_ms=" << percentile(stats.latency_ms, 0.99);
    logger->line(line.str());
}

}  // namespace

int main(int argc, char* argv[]) {
    Options options;
    if (!parseOptions(argc, argv, &options)) {
        return argc > 1 && std::string(argv[1]) == "--help" ? 0 : 1;
    }

    const auto run_id = std::chrono::duration_cast<std::chrono::milliseconds>(
        SystemClock::now().time_since_epoch()).count();
    Logger logger(options.log_file);
    logger.line(
        "[stage] address=" + options.address +
        " batch_size=" + std::to_string(options.batch_size) +
        " batches=" + std::to_string(options.batches) +
        " query_repetitions=" + std::to_string(options.query_repetitions));

    const auto channel = ::grpc::CreateChannel(
        options.address, ::grpc::InsecureChannelCredentials());
    const auto resolve_ids = makeIds("resolve", run_id);
    const auto raw_ids = makeIds("raw", run_id);
    const auto window_ids = makeIds("window", run_id);
    const auto full_ids = makeIds("full", run_id);
    std::vector<std::string> all_ids;
    all_ids.insert(all_ids.end(), resolve_ids.begin(), resolve_ids.end());
    all_ids.insert(all_ids.end(), raw_ids.begin(), raw_ids.end());
    all_ids.insert(all_ids.end(), window_ids.begin(), window_ids.end());
    all_ids.insert(all_ids.end(), full_ids.begin(), full_ids.end());
    if (!syncInstances(channel, all_ids, &logger)) {
        return 2;
    }
    if (!syncWindowConfig(channel, options.window_size_ms, &logger)) {
        return 2;
    }

    const auto base_time = std::chrono::duration_cast<
        std::chrono::milliseconds>(SystemClock::now().time_since_epoch()).count();
    const auto range = static_cast<std::int64_t>(
        options.batches * options.batch_size + 10'000);
    const auto resolve_base = base_time;
    const auto raw_base = base_time + range;
    const auto window_base = base_time + 2 * range;
    const auto full_base = base_time + 3 * range;

    StageStats resolve_stats;
    StageStats raw_stats;
    StageStats window_stats;
    StageStats full_stats;
    StageStats history_stats;
    StageStats overview_stats;
    runResolve(channel, resolve_ids, options, resolve_base, &resolve_stats, &logger);
    runRawWrite(channel, raw_ids, options, raw_base, &raw_stats, &logger);
    runWindowBuild(
        channel, window_ids, options, window_base, &window_stats, &logger);
    runFullIngest(channel, full_ids, options, full_base, &full_stats, &logger);
    runHistoryQueries(
        channel, full_ids, options, full_base,
        &history_stats, &overview_stats, &logger);

    logStats("ingestAndResolveData", resolve_stats, &logger);
    logStats("writeRawData", raw_stats, &logger);
    logStats("buildTimeWindow", window_stats, &logger);
    logStats("ingestData", full_stats, &logger);
    logStats("queryHistoryData", history_stats, &logger);
    logStats("queryHistoryOverview", overview_stats, &logger);

    std::error_code error;
    const fs::path output_path(options.output_file);
    if (!output_path.parent_path().empty()) {
        fs::create_directories(output_path.parent_path(), error);
    }
    std::ofstream output(options.output_file, std::ios::out | std::ios::trunc);
    if (output.is_open()) {
        output << "rpc,calls,successes,failures,avg_ms,p50_ms,p95_ms,p99_ms\n";
        const auto write_row = [&](const char* name, const StageStats& stats) {
            const double average = stats.latency_ms.empty()
                ? 0.0
                : std::accumulate(
                      stats.latency_ms.begin(), stats.latency_ms.end(), 0.0) /
                    static_cast<double>(stats.latency_ms.size());
            output << name << ',' << stats.calls << ',' << stats.successes << ','
                   << stats.failures << ',' << average << ','
                   << percentile(stats.latency_ms, 0.50) << ','
                   << percentile(stats.latency_ms, 0.95) << ','
                   << percentile(stats.latency_ms, 0.99) << '\n';
        };
        write_row("ingestAndResolveData", resolve_stats);
        write_row("writeRawData", raw_stats);
        write_row("buildTimeWindow", window_stats);
        write_row("ingestData", full_stats);
        write_row("queryHistoryData", history_stats);
        write_row("queryHistoryOverview", overview_stats);
        logger.line("[stage] csv_result=" + options.output_file);
    }

    const auto total_failures = resolve_stats.failures + raw_stats.failures +
        window_stats.failures + full_stats.failures + history_stats.failures +
        overview_stats.failures;
    return total_failures == 0 ? 0 : 3;
}
