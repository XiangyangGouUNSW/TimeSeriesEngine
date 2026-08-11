#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <future>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <unistd.h>

#include "sfkg/timeseries/core/derived_series_service.hpp"
#include "sfkg/timeseries/core/grpc/ingest_task_executor.hpp"
#include "sfkg/timeseries/core/ingest_service.hpp"
#include "sfkg/timeseries/core/internal/taos_client.hpp"
#include "sfkg/timeseries/core/runtime_config_registry.hpp"
#include "sfkg/timeseries/core/window_service.hpp"

namespace core = sfkg::timeseries::core;
namespace grpc_core = sfkg::timeseries::core::grpc;

namespace {

using Clock = std::chrono::steady_clock;

std::size_t parseSize(
    const char* text,
    std::size_t fallback) {
    if (text == nullptr || *text == '\0') {
        return fallback;
    }
    std::size_t value = 0;
    for (const char* cursor = text; *cursor != '\0'; ++cursor) {
        if (*cursor < '0' || *cursor > '9') {
            return fallback;
        }
        const auto digit = static_cast<std::size_t>(*cursor - '0');
        if (value > (std::numeric_limits<std::size_t>::max() - digit) / 10) {
            return fallback;
        }
        value = value * 10 + digit;
    }
    return value == 0 ? fallback : value;
}

std::size_t argumentOr(
    int argc,
    char** argv,
    int index,
    std::size_t fallback) {
    return index < argc ? parseSize(argv[index], fallback) : fallback;
}

double elapsedMs(Clock::time_point start, Clock::time_point end) {
    return std::chrono::duration<double, std::milli>(end - start).count();
}

struct LaneStats {
    std::size_t batches{0};
    std::size_t points{0};
    double total_ms{0.0};
    double max_ms{0.0};
};

struct SharedStats {
    explicit SharedStats(std::size_t writer_count)
        : cold(writer_count) {}

    std::mutex mutex;
    std::vector<LaneStats> cold;
    LaneStats hot;
    std::size_t routing_mismatches{0};
    std::unordered_map<core::SequenceId, std::size_t> observed_writers;
};

struct TaskMetrics {
    Clock::time_point submitted{};
    Clock::time_point cold_first_start{};
    Clock::time_point cold_last_end{};
    Clock::time_point hot_start{};
    Clock::time_point hot_end{};
    Clock::time_point completed{};
    std::size_t cold_shards{0};
    std::size_t expected_lanes{0};
    std::size_t completed_lanes{0};
    double resolve_ms{0.0};
    std::mutex mutex;
};

class DatabaseCleanup final {
public:
    explicit DatabaseCleanup(core::internal::TaosClient& client)
        : client_(client) {}

    ~DatabaseCleanup() {
        core::OperationResult result;
        for (int attempt = 0; attempt < 5; ++attempt) {
            result = client_.dropDatabaseForTesting();
            if (result.code == core::OperationCode::Ok) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        std::cerr << "temporary benchmark database cleanup failed: "
                  << result.message << '\n';
    }

private:
    core::internal::TaosClient& client_;
};

void printPercentiles(
    const std::string& name,
    std::vector<double> values) {
    if (values.empty()) {
        std::cout << name << ": no samples\n";
        return;
    }
    std::sort(values.begin(), values.end());
    const auto percentile = [&values](double ratio) {
        const auto index = static_cast<std::size_t>(
            ratio * static_cast<double>(values.size() - 1));
        return values[index];
    };
    const auto total = std::accumulate(values.begin(), values.end(), 0.0);
    std::cout << std::fixed << std::setprecision(2)
              << name << ": count=" << values.size()
              << " avg_ms=" << total / static_cast<double>(values.size())
              << " p50_ms=" << percentile(0.50)
              << " p95_ms=" << percentile(0.95)
              << " max_ms=" << values.back() << '\n';
}

}  // namespace

int main(int argc, char** argv) {
    // Arguments: batch_count points_per_batch sequence_count producer_count
    // writer_count.
    // Defaults process 100,000 points: 100 batches * 1,000 points.
    const auto batch_count = argumentOr(argc, argv, 1, 100);
    const auto points_per_batch = argumentOr(argc, argv, 2, 1000);
    const auto sequence_count = argumentOr(argc, argv, 3, 20);
    const auto producer_count = argumentOr(argc, argv, 4, 4);
    const auto writer_count = argumentOr(argc, argv, 5, 4);

    if (points_per_batch < sequence_count ||
        points_per_batch % sequence_count != 0) {
        std::cerr << "points_per_batch must be divisible by sequence_count\n";
        return 2;
    }

    const auto points_per_sequence = points_per_batch / sequence_count;
    const auto temporary_database =
        std::string("sfkg_local_parallel_benchmark_") +
        std::to_string(static_cast<long long>(getpid()));
    setenv("SFKG_TAOS_HOST", "127.0.0.1", 1);
    setenv("SFKG_TAOS_PORT", "6030", 1);
    setenv("SFKG_TAOS_USER", "root", 1);
    setenv("SFKG_TAOS_PASSWORD", "taosdata", 1);
    setenv("SFKG_TAOS_DB", temporary_database.c_str(), 1);
    setenv("SFKG_TAOS_KEEP_DAYS", "365000", 1);
    const auto writer_count_text = std::to_string(writer_count);
    setenv("SFKG_TAOS_WRITE_CONNECTIONS", writer_count_text.c_str(), 1);
    setenv("SFKG_INGEST_COLD_WORKERS", writer_count_text.c_str(), 1);
    setenv("SFKG_INGEST_HOT_WORKERS", "1", 1);
    setenv("SFKG_INGEST_QUEUE_CAPACITY", "100000", 1);

    core::internal::TaosClient taos_client;
    DatabaseCleanup cleanup(taos_client);
    const auto schema = taos_client.ensureSchema();
    if (schema.code != core::OperationCode::Ok) {
        std::cerr << "failed to initialize benchmark schema: "
                  << schema.message << '\n';
        return 1;
    }

    core::RuntimeConfigRegistry registry;
    core::RuntimeConfigSnapshot<core::RuntimeInstanceConfig> snapshot;
    const std::vector<core::SequenceId> etth1_sequence_ids{
        "ETTh1_HUFL", "ETTh1_HULL", "ETTh1_MUFL", "ETTh1_MULL",
        "ETTh1_LUFL", "ETTh1_LULL", "ETTh1_OT"};
    const bool use_etth1_names = sequence_count == etth1_sequence_ids.size();
    std::vector<core::SequenceId> sequence_ids;
    sequence_ids.reserve(sequence_count);
    for (std::size_t index = 0; index < sequence_count; ++index) {
        const auto sequence_id = use_etth1_names
            ? etth1_sequence_ids[index]
            : "benchmark-sequence-" + std::to_string(index);
        sequence_ids.push_back(sequence_id);
        snapshot.items.push_back({
            sequence_id,
            "local-benchmark",
            "external-" + std::to_string(index),
            "benchmark-category",
            "double"});
    }
    const auto configured = registry.replaceInstanceConfigs(snapshot);
    if (configured.code != core::OperationCode::Ok) {
        std::cerr << "failed to register benchmark sequences: "
                  << configured.message << '\n';
        return 1;
    }

    core::IngestService ingest_service(registry);
    core::WindowService window_service;
    core::DerivedSeriesService derived_service(registry, window_service);
    grpc_core::IngestTaskExecutor executor;
    SharedStats stats(writer_count);
    std::vector<std::pair<
        std::future<grpc_core::IngestPipelineResult>,
        std::shared_ptr<TaskMetrics>>> submissions;
    std::mutex result_mutex;

    std::cout << "local ingest parallel benchmark\n"
              << "database=" << temporary_database << '\n'
              << "batches=" << batch_count
              << " points_per_batch=" << points_per_batch
              << " sequences=" << sequence_count
              << " producers=" << producer_count << '\n'
              << "cold_workers=" << writer_count
              << " taos_write_connections=" << writer_count
              << " hot_workers=1\n";

    const auto wall_start = Clock::now();
    std::vector<std::thread> producers;
    producers.reserve(producer_count);
    for (std::size_t producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            for (std::size_t batch_index = producer;
                 batch_index < batch_count;
                 batch_index += producer_count) {
                std::vector<core::TimeseriesIngestData> input;
                input.reserve(points_per_batch);
                const auto base_time = static_cast<core::Timestamp>(
                    1'900'000'000'000LL +
                    batch_index * points_per_sequence);
                for (std::size_t sequence_index = 0;
                     sequence_index < sequence_ids.size();
                     ++sequence_index) {
                    for (std::size_t point_index = 0;
                         point_index < points_per_sequence;
                         ++point_index) {
                        input.push_back({
                            sequence_ids[sequence_index],
                            {},
                            {},
                            base_time + static_cast<core::Timestamp>(point_index),
                            static_cast<double>(
                                sequence_index * 1000000 +
                                batch_index * points_per_sequence +
                                point_index)});
                    }
                }

                const auto resolve_start = Clock::now();
                auto resolved = std::make_shared<core::IngestResult>(
                    ingest_service.ingestAndResolveData(input));
                const auto resolve_end = Clock::now();
                if (resolved->operation.code != core::OperationCode::Ok) {
                    std::cerr << "resolve failed for batch " << batch_index
                              << ": " << resolved->operation.message << '\n';
                    continue;
                }

                auto task_metrics = std::make_shared<TaskMetrics>();
                task_metrics->resolve_ms = elapsedMs(resolve_start, resolve_end);
                task_metrics->submitted = Clock::now();
                task_metrics->expected_lanes = 1 + std::min(
                    writer_count, sequence_ids.size());
                auto submission = executor.trySubmit(
                    resolved,
                    [&, task_metrics](
                        std::size_t writer_index,
                        const core::TimeseriesBatch& data) {
                        const auto start = Clock::now();
                        const auto result = taos_client.insertRawOnConnection(
                            writer_index, data);
                        const auto end = Clock::now();
                        {
                            std::lock_guard lock(stats.mutex);
                            auto& lane = stats.cold[writer_index];
                            ++lane.batches;
                            lane.points += data.points.size();
                            const auto duration = elapsedMs(start, end);
                            lane.total_ms += duration;
                            lane.max_ms = std::max(lane.max_ms, duration);
                            for (const auto& point : data.points) {
                                const auto [existing, inserted] =
                                    stats.observed_writers.emplace(
                                        point.sequence_id, writer_index);
                                if (!inserted && existing->second != writer_index) {
                                    ++stats.routing_mismatches;
                                }
                            }
                        }
                        {
                            std::lock_guard lock(task_metrics->mutex);
                            if (task_metrics->cold_shards == 0 ||
                                start < task_metrics->cold_first_start) {
                                task_metrics->cold_first_start = start;
                            }
                            if (task_metrics->cold_shards == 0 ||
                                end > task_metrics->cold_last_end) {
                                task_metrics->cold_last_end = end;
                            }
                            ++task_metrics->cold_shards;
                            ++task_metrics->completed_lanes;
                            if (task_metrics->completed_lanes ==
                                task_metrics->expected_lanes) {
                                task_metrics->completed = end;
                            }
                        }
                        return result;
                    },
                    [&, task_metrics](const core::TimeseriesBatch& data) {
                        const auto start = Clock::now();
                        auto result = grpc_core::IngestPipelineResult{};
                        result.window_result = window_service.buildTimeWindow(data);
                        if (result.window_result.code == core::OperationCode::Ok) {
                            result.derived_result = derived_service.refresh();
                        } else {
                            result.derived_result = result.window_result;
                        }
                        result.constraint_notification_result = {
                            core::OperationCode::Ok,
                            0,
                            0,
                            "constraint notification omitted in local benchmark"};
                        const auto end = Clock::now();
                        {
                            std::lock_guard lock(stats.mutex);
                            ++stats.hot.batches;
                            stats.hot.points += data.points.size();
                            const auto duration = elapsedMs(start, end);
                            stats.hot.total_ms += duration;
                            stats.hot.max_ms = std::max(stats.hot.max_ms, duration);
                        }
                        {
                            std::lock_guard lock(task_metrics->mutex);
                            task_metrics->hot_start = start;
                            task_metrics->hot_end = end;
                            ++task_metrics->completed_lanes;
                            if (task_metrics->completed_lanes ==
                                task_metrics->expected_lanes) {
                                task_metrics->completed = end;
                            }
                        }
                        return result;
                    });

                if (!submission.accepted) {
                    std::cerr << "batch " << batch_index
                              << " was rejected: "
                              << submission.admission.message << '\n';
                    continue;
                }
                {
                    std::lock_guard lock(result_mutex);
                    submissions.emplace_back(
                        std::move(submission.completion), task_metrics);
                }
            }
        });
    }
    for (auto& producer : producers) {
        producer.join();
    }
    for (auto& submission : submissions) {
        const auto result = submission.first.get();
        {
            std::lock_guard lock(submission.second->mutex);
            if (submission.second->completed == Clock::time_point{}) {
                submission.second->completed = Clock::now();
            }
        }
        if (result.storage_result.code != core::OperationCode::Ok ||
            result.window_result.code != core::OperationCode::Ok) {
            std::cerr << "benchmark task failed: storage="
                      << result.storage_result.message
                      << " window=" << result.window_result.message << '\n';
        }
    }
    const auto wall_end = Clock::now();

    std::vector<double> resolve_times;
    std::vector<double> cold_times;
    std::vector<double> hot_times;
    std::vector<double> total_times;
    std::vector<std::shared_ptr<TaskMetrics>> metrics;
    metrics.reserve(submissions.size());
    for (const auto& submission : submissions) {
        metrics.push_back(submission.second);
    }
    resolve_times.reserve(metrics.size());
    cold_times.reserve(metrics.size());
    hot_times.reserve(metrics.size());
    total_times.reserve(metrics.size());
    for (const auto& task : metrics) {
        std::lock_guard lock(task->mutex);
        resolve_times.push_back(task->resolve_ms);
        cold_times.push_back(
            elapsedMs(task->cold_first_start, task->cold_last_end));
        hot_times.push_back(elapsedMs(task->hot_start, task->hot_end));
        total_times.push_back(elapsedMs(task->submitted, task->completed));
    }

    std::cout << "\ntiming summary (milliseconds):\n";
    printPercentiles("resolve", std::move(resolve_times));
    printPercentiles("cold shard span", std::move(cold_times));
    printPercentiles("hot window+derived", std::move(hot_times));
    printPercentiles("submit-to-completion", std::move(total_times));

    std::size_t written_points = 0;
    std::vector<std::size_t> observed_counts(writer_count, 0);
    std::cout << "\nobserved sequence routing:\n";
    for (const auto& sequence_id : sequence_ids) {
        const auto found = stats.observed_writers.find(sequence_id);
        if (found != stats.observed_writers.end()) {
            ++observed_counts[found->second];
            std::cout << "  " << sequence_id
                      << " -> writer=" << found->second << '\n';
        }
    }
    for (std::size_t writer = 0; writer < writer_count; ++writer) {
        std::cout << "  writer=" << writer
                  << " assigned_sequences=" << observed_counts[writer]
                  << '\n';
    }
    std::cout << "\ncold writer summary:\n";
    for (std::size_t writer = 0; writer < writer_count; ++writer) {
        const auto& lane = stats.cold[writer];
        written_points += lane.points;
        std::cout << std::fixed << std::setprecision(2)
                  << "  writer=" << writer
                  << " batches=" << lane.batches
                  << " points=" << lane.points
                  << " avg_write_ms="
                  << (lane.batches == 0
                          ? 0.0
                          : lane.total_ms /
                              static_cast<double>(lane.batches))
                  << " max_write_ms=" << lane.max_ms << '\n';
    }
    const auto wall_ms = elapsedMs(wall_start, wall_end);
    const auto expected_points = batch_count * points_per_batch;
    std::cout << "  routing_mismatches=" << stats.routing_mismatches << '\n'
              << "  expected_points=" << expected_points
              << " written_points=" << written_points
              << " hot_points=" << stats.hot.points << '\n'
              << std::fixed << std::setprecision(2)
              << "wall_ms=" << wall_ms
              << " throughput_points_per_sec="
              << (wall_ms == 0.0
                      ? 0.0
                      : static_cast<double>(written_points) * 1000.0 /
                          wall_ms)
              << '\n';

    if (written_points != expected_points ||
        stats.hot.points != expected_points ||
        stats.routing_mismatches != 0) {
        return 1;
    }
    return 0;
}
