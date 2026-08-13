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
#include "sfkg/timeseries/core/alignment_service.hpp"
#include "sfkg/timeseries/core/constraint_check_engine.hpp"
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
    double hot_window_ms{0.0};
    double derived_ms{0.0};
    double constraint_query_ms{0.0};
    double constraint_check_ms{0.0};
    bool incremental_safe{false};
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
    // writer_count window_size_ms profile hot_worker_count.
    // Defaults process 100,000 points: 100 batches * 1,000 points.
    const auto batch_count = argumentOr(argc, argv, 1, 100);
    const auto points_per_batch = argumentOr(argc, argv, 2, 1000);
    const auto sequence_count = argumentOr(argc, argv, 3, 20);
    const auto producer_count = argumentOr(argc, argv, 4, 4);
    const auto writer_count = argumentOr(argc, argv, 5, 8);
    const auto window_size_ms = argumentOr(argc, argv, 6, 3'600'000);
    const std::string profile = argc > 7 ? argv[7] : "none";
    const auto hot_worker_count = argumentOr(argc, argv, 8, 1);

    if (profile != "none" && profile != "single" &&
        profile != "standard" && profile != "mixed") {
        std::cerr << "profile must be none, single, standard or mixed\n";
        return 2;
    }

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
    const auto hot_worker_count_text = std::to_string(hot_worker_count);
    setenv("SFKG_INGEST_HOT_WORKERS", hot_worker_count_text.c_str(), 1);
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
            "double",
            core::SeriesKind::Continuous});
    }
    const auto configured = registry.replaceInstanceConfigs(snapshot);
    if (configured.code != core::OperationCode::Ok) {
        std::cerr << "failed to register benchmark sequences: "
                  << configured.message << '\n';
        return 1;
    }

    const bool with_single = profile == "single" ||
        profile == "standard" || profile == "mixed";
    const bool with_multi_constraints = profile == "standard" ||
        profile == "mixed";
    const bool with_multi_derived = profile == "mixed";
    std::vector<core::ConstraintRule> single_rules;
    std::vector<core::ConstraintRule> multi_rules;
    if (with_single) {
        core::ConstraintRule rule;
        rule.constraint_id = "local-single-range";
        rule.variable_mapping.emplace("ot", "ETTh1_OT");
        rule.lower_bound = -1.0e12;
        rule.upper_bound = 1.0e12;
        rule.terms.push_back({"ot", 1.0, 0});
        single_rules.push_back(rule);
    }
    if (with_multi_constraints) {
        core::ConstraintRule rule;
        rule.constraint_id = "local-multi-range";
        rule.variable_mapping.emplace("hufl", "ETTh1_HUFL");
        rule.variable_mapping.emplace("hull", "ETTh1_HULL");
        rule.lower_bound = -1.0e12;
        rule.upper_bound = 1.0e12;
        rule.terms.push_back({"hufl", 1.0, 0});
        rule.terms.push_back({"hull", 1.0, 0});
        multi_rules.push_back(rule);
    }
    if (with_single || with_multi_constraints) {
        core::RuntimeConfigSnapshot<core::RuntimeConstraintConfig>
            constraint_snapshot;
        for (const auto& rule : single_rules) {
            constraint_snapshot.items.push_back({rule, true});
        }
        for (const auto& rule : multi_rules) {
            constraint_snapshot.items.push_back({rule, true});
        }
        const auto constraints_configured = registry.upsertConstraints(
            constraint_snapshot);
        if (constraints_configured.code != core::OperationCode::Ok) {
            std::cerr << "failed to register benchmark constraints: "
                      << constraints_configured.message << '\n';
            return 1;
        }
    }
    if (with_single || with_multi_derived) {
        core::RuntimeConfigSnapshot<core::RuntimeDerivedSeriesConfig>
            derived_snapshot;
        if (with_single) {
            derived_snapshot.items.push_back({
                "ETTh1_OT_DERIVED",
                true,
                core::DerivedLinearCombination{{
                    {"ETTh1_OT", 1.0}}, 0.0}});
        }
        if (with_multi_derived) {
            derived_snapshot.items.push_back({
                "ETTh1_HUFL_HULL_DERIVED",
                true,
                core::DerivedLinearCombination{{
                    {"ETTh1_HUFL", 1.0},
                    {"ETTh1_HULL", 1.0}}, 0.0}});
        }
        const auto derived_configured = registry.upsertDerivedSeriesConfigs(
            derived_snapshot);
        if (derived_configured.code != core::OperationCode::Ok) {
            std::cerr << "failed to register benchmark derived configs: "
                      << derived_configured.message << '\n';
            return 1;
        }
    }

    core::IngestService ingest_service(registry);
    core::WindowService window_service;
    const auto window_configured =
        window_service.configureWindowSize(window_size_ms);
    if (window_configured.code != core::OperationCode::Ok) {
        std::cerr << "failed to configure benchmark window: "
                  << window_configured.message << '\n';
        return 1;
    }
    core::DerivedSeriesService derived_service(registry, window_service);
    core::AlignmentService alignment_service(registry);
    core::ConstraintCheckEngine constraint_engine;
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
              << " window_size_ms=" << window_size_ms
              << " profile=" << profile
              << " hot_workers=" << hot_worker_count << '\n'
              << "cold_workers=" << writer_count
              << " taos_write_connections=" << writer_count
              << " hot_workers=" << hot_worker_count << '\n';

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
                    batch_index * points_per_sequence * 1'000);
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
                            // ETTh1-style sampling: one row per second and
                            // one point per configured sequence in that row.
                            base_time + static_cast<core::Timestamp>(point_index) *
                                1'000,
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
                            lane.points += result.success_count;
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
                        const auto window_start = Clock::now();
                        const auto window_update =
                            window_service.buildTimeWindowIncremental(data);
                        result.window_result = window_update.operation;
                        const auto window_end = Clock::now();
                        {
                            std::lock_guard lock(task_metrics->mutex);
                            task_metrics->hot_window_ms = elapsedMs(
                                window_start, window_end);
                                task_metrics->incremental_safe =
                                    window_update.incremental_safe;
                        }
                        std::future<core::OperationResult> derived_future;
                        const auto derived_start = Clock::now();
                        if (result.window_result.code == core::OperationCode::Ok) {
                            if (!single_rules.empty() || !multi_rules.empty()) {
                                derived_future = std::async(
                                    std::launch::async,
                                    [&derived_service, window_update] {
                                        return derived_service.refresh(
                                            window_update);
                                    });
                            } else {
                                result.derived_result = derived_service.refresh(
                                    window_update);
                            }
                        } else {
                            result.derived_result = result.window_result;
                        }
                        result.constraint_notification_result =
                            core::OperationResult{
                                core::OperationCode::Ok,
                                0,
                                0,
                                "constraint notification omitted in local benchmark"};
                        if (result.window_result.code == core::OperationCode::Ok &&
                            (!single_rules.empty() || !multi_rules.empty())) {
                            core::WindowQuery single_query;
                            core::WindowQuery multi_query;
                            std::size_t single_max_offset = 0;
                            const auto addUnique = [](
                                std::vector<core::SequenceId>* ids,
                                const core::SequenceId& id) {
                                if (std::find(ids->begin(), ids->end(), id) ==
                                    ids->end()) {
                                    ids->push_back(id);
                                }
                            };
                            for (const auto& rule : single_rules) {
                                for (const auto& [variable, sequence_id] :
                                     rule.variable_mapping) {
                                    (void)variable;
                                    addUnique(&single_query.sequence_ids,
                                              sequence_id);
                                }
                                for (const auto& term : rule.terms) {
                                    single_max_offset = std::max(
                                        single_max_offset, term.sample_offset);
                                }
                            }
                            for (const auto& rule : multi_rules) {
                                for (const auto& [variable, sequence_id] :
                                     rule.variable_mapping) {
                                    (void)variable;
                                    addUnique(&multi_query.sequence_ids,
                                              sequence_id);
                                }
                            }
                            if (window_update.incremental_safe &&
                                window_update.affected_start_time &&
                                window_update.affected_end_time) {
                                single_query.start_time =
                                    *window_update.affected_start_time;
                                single_query.end_time =
                                    *window_update.affected_end_time ==
                                            std::numeric_limits<core::Timestamp>::max()
                                        ? *window_update.affected_end_time
                                        : *window_update.affected_end_time + 1;
                                single_query.preceding_points =
                                    single_max_offset + 1;
                                single_query.following_points =
                                    single_max_offset + 1;
                            } else {
                                single_query = multi_query;
                            }

                            struct GroupExecution {
                                core::ConstraintCheckResult result;
                                double query_ms{0.0};
                                double check_ms{0.0};
                            };
                            const auto runGroup = [&](
                                const std::vector<core::ConstraintRule>& rules,
                                const core::WindowQuery& query) {
                                GroupExecution execution;
                                if (rules.empty()) {
                                    execution.result.satisfied = true;
                                    execution.result.operation = core::OperationResult{
                                        core::OperationCode::Ok, 0, 0,
                                        "constraint group skipped"};
                                    return execution;
                                }
                                const auto query_start = Clock::now();
                                const auto window = window_service.queryWindowData(
                                    query);
                                const auto query_end = Clock::now();
                                execution.query_ms = elapsedMs(
                                    query_start, query_end);
                                if (window.operation.code !=
                                    core::OperationCode::Ok) {
                                    execution.result.satisfied = false;
                                    execution.result.operation = window.operation;
                                    return execution;
                                }
                                const auto check_start = Clock::now();
                                std::optional<core::ConstraintCheckRange> check_range;
                                if (window_update.incremental_safe &&
                                    window_update.affected_start_time &&
                                    window_update.affected_end_time) {
                                    check_range = core::ConstraintCheckRange{
                                        *window_update.affected_start_time,
                                        *window_update.affected_end_time};
                                }
                                if (&rules == &single_rules) {
                                    execution.result = check_range
                                        ? constraint_engine.checkConstraints(
                                              rules, window.data, check_range)
                                        : constraint_engine.checkConstraints(
                                              rules, window.data);
                                } else {
                                    const auto alignment = check_range
                                        ? alignment_service.alignWindowData(
                                              window.data,
                                              core::AlignmentRange{
                                                  check_range->start_time,
                                                  check_range->end_time,
                                                  0})
                                        : alignment_service.alignWindowData(
                                              window.data);
                                    execution.result = alignment.operation.code ==
                                            core::OperationCode::Ok ||
                                            alignment.operation.code ==
                                                core::OperationCode::PartialSuccess
                                        ? (check_range
                                            ? constraint_engine.checkConstraints(
                                                  rules,
                                                  alignment.aligned_data,
                                                  check_range)
                                            : constraint_engine.checkConstraints(
                                                  rules,
                                                  alignment.aligned_data))
                                        : core::ConstraintCheckResult{
                                              alignment.operation, 0, false, {}};
                                }
                                const auto check_end = Clock::now();
                                execution.check_ms = elapsedMs(
                                    check_start, check_end);
                                return execution;
                            };

                            GroupExecution single_execution;
                            GroupExecution multi_execution;
                            if (!single_rules.empty() && !multi_rules.empty()) {
                                auto single_future = std::async(
                                    std::launch::async,
                                    runGroup,
                                    std::cref(single_rules),
                                    std::cref(single_query));
                                auto multi_future = std::async(
                                    std::launch::async,
                                    runGroup,
                                    std::cref(multi_rules),
                                    std::cref(multi_query));
                                single_execution = single_future.get();
                                multi_execution = multi_future.get();
                            } else if (!single_rules.empty()) {
                                single_execution = runGroup(
                                    single_rules, single_query);
                            } else {
                                multi_execution = runGroup(
                                    multi_rules, multi_query);
                            }
                            {
                                std::lock_guard lock(task_metrics->mutex);
                                task_metrics->constraint_query_ms =
                                    single_execution.query_ms +
                                    multi_execution.query_ms;
                                task_metrics->constraint_check_ms =
                                    single_execution.check_ms +
                                    multi_execution.check_ms;
                            }
                            if ((!single_rules.empty() &&
                                 single_execution.result.operation.code !=
                                     core::OperationCode::Ok) ||
                                (!multi_rules.empty() &&
                                 multi_execution.result.operation.code !=
                                     core::OperationCode::Ok)) {
                                result.constraint_notification_result =
                                    core::OperationResult{
                                        core::OperationCode::InternalError,
                                        0,
                                        1,
                                        "local constraint benchmark check failed"};
                            }
                        }
                        if (derived_future.valid()) {
                            result.derived_result = derived_future.get();
                        }
                        const auto derived_end = Clock::now();
                        {
                            std::lock_guard lock(task_metrics->mutex);
                            task_metrics->derived_ms = elapsedMs(
                                derived_start, derived_end);
                        }
                        const auto end = Clock::now();
                        {
                            std::lock_guard lock(stats.mutex);
                            ++stats.hot.batches;
                            if (result.window_result.code ==
                                    core::OperationCode::Ok ||
                                result.window_result.code ==
                                    core::OperationCode::PartialSuccess) {
                                stats.hot.points += data.points.size();
                            }
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
    std::vector<double> hot_window_times;
    std::vector<double> derived_times;
    std::vector<double> constraint_query_times;
    std::vector<double> constraint_check_times;
    std::vector<double> total_times;
    std::size_t incremental_safe_count = 0;
    std::vector<std::shared_ptr<TaskMetrics>> metrics;
    metrics.reserve(submissions.size());
    for (const auto& submission : submissions) {
        metrics.push_back(submission.second);
    }
    resolve_times.reserve(metrics.size());
    cold_times.reserve(metrics.size());
    hot_times.reserve(metrics.size());
    hot_window_times.reserve(metrics.size());
    derived_times.reserve(metrics.size());
    constraint_query_times.reserve(metrics.size());
    constraint_check_times.reserve(metrics.size());
    total_times.reserve(metrics.size());
    for (const auto& task : metrics) {
        std::lock_guard lock(task->mutex);
        resolve_times.push_back(task->resolve_ms);
        cold_times.push_back(
            elapsedMs(task->cold_first_start, task->cold_last_end));
        hot_times.push_back(elapsedMs(task->hot_start, task->hot_end));
        hot_window_times.push_back(task->hot_window_ms);
        derived_times.push_back(task->derived_ms);
        constraint_query_times.push_back(task->constraint_query_ms);
        constraint_check_times.push_back(task->constraint_check_ms);
        if (task->incremental_safe) {
            ++incremental_safe_count;
        }
        total_times.push_back(elapsedMs(task->submitted, task->completed));
    }

    std::cout << "\ntiming summary (milliseconds):\n";
    printPercentiles("resolve", std::move(resolve_times));
    printPercentiles("cold shard span", std::move(cold_times));
    printPercentiles("hot total", std::move(hot_times));
    printPercentiles("hot window", std::move(hot_window_times));
    printPercentiles("derived", std::move(derived_times));
    printPercentiles("constraint query", std::move(constraint_query_times));
    printPercentiles("constraint check", std::move(constraint_check_times));
    std::cout << "incremental_safe_batches=" << incremental_safe_count
              << " incremental_fallback_batches="
              << (metrics.size() - incremental_safe_count) << '\n';
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
    const auto expected_rows = expected_points / sequence_count;
    std::cout << "  routing_mismatches=" << stats.routing_mismatches << '\n'
              << "  expected_rows(timestamps)=" << expected_rows << '\n'
              << "  expected_points=" << expected_points
              << " written_points=" << written_points
              << " hot_points=" << stats.hot.points << '\n'
              << std::fixed << std::setprecision(2)
              << "wall_ms=" << wall_ms
              << " throughput_rows_per_sec="
              << (wall_ms == 0.0
                      ? 0.0
                      : static_cast<double>(expected_rows) * 1000.0 /
                          wall_ms)
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
