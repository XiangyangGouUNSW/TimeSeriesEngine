#pragma once

#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core {

// Describes the part of the hot window affected by one raw-data update.
// affected_end_time is inclusive. incremental_safe describes ordering and
// correction safety; window eviction is reported independently because it can
// be handled incrementally by removing the expired prefix and patching the
// newly affected suffix. Out-of-order corrections can still make
// incremental_safe false because they may change interpolation or ordering
// dependencies outside the incoming suffix.
struct SequenceWindowUpdate {
    std::optional<Timestamp> affected_start_time;
    std::optional<Timestamp> affected_end_time;
    bool incremental_safe{false};
    bool window_evicted{false};
};

struct WindowUpdateResult {
    OperationResult operation;
    ProjectId project_id;
    std::vector<SequenceId> changed_sequence_ids;
    // Incremental consumers must use the status for their dependency set,
    // not the aggregate flag below. A derived formula or constraint can stay
    // incremental when an unrelated sequence received a late correction.
    std::unordered_map<SequenceId, SequenceWindowUpdate> sequence_updates;
    std::optional<Timestamp> affected_start_time;
    std::optional<Timestamp> affected_end_time;
    std::optional<Timestamp> window_start_time;
    // Monotonically increases for each accepted batch. Derived refreshes use
    // it to prevent an older concurrent refresh from publishing over a newer
    // one after hot-window work has been sharded by sequence.
    std::uint64_t update_generation{0};
    bool incremental_safe{false};
    bool window_evicted{false};
    // These are diagnostic sums across sequence tasks. They may be larger
    // than the wall-clock hot_window_ms when several sequences are processed
    // concurrently. In particular, lock_wait_ms measures contention rather
    // than time spent modifying the vector/tree itself.
    double sequence_lock_wait_ms{0.0};
    double sequence_update_ms{0.0};
    // sequence_task_count is the number of independent sequence updates;
    // sequence_group_count is the number of executor tasks after small
    // sequence updates have been coalesced by estimated work.
    std::size_t sequence_task_count{0};
    std::size_t sequence_group_count{0};
    double eviction_lock_wait_ms{0.0};
    double eviction_update_ms{0.0};
};

class WindowService {
public:
    static constexpr std::int64_t kDefaultWindowSizeMs =
        ::sfkg::timeseries::core::kDefaultWindowSizeMs;

    WindowService();
    ~WindowService();

    WindowService(const WindowService&) = delete;
    WindowService& operator=(const WindowService&) = delete;

    OperationResult configureWindowSize(
        const ProjectId& project_id,
        std::int64_t window_size);
    OperationResult configureWindowSize(std::int64_t window_size);
    std::int64_t windowSize(const ProjectId& project_id) const;
    std::int64_t windowSize() const;

    // Uses the runtime-configured window size.
    OperationResult buildTimeWindow(const TimeseriesBatch& data);
    OperationResult buildTimeWindow(
        const ProjectId& project_id,
        const TimeseriesBatch& data);

    // Explicit size is retained for fine-grained tests, replay and
    // compensation flows.
    OperationResult buildTimeWindow(
        const ProjectId& project_id,
        const TimeseriesBatch& data,
        std::int64_t window_size);
    OperationResult buildTimeWindow(
        const TimeseriesBatch& data,
        std::int64_t window_size);

    // Same hot-window update as buildTimeWindow(), with change information
    // for incremental derived-series and constraint processing.
    WindowUpdateResult buildTimeWindowIncremental(
        const ProjectId& project_id,
        const TimeseriesBatch& data);
    WindowUpdateResult buildTimeWindowIncremental(
        const TimeseriesBatch& data);

    // Replaces a derived sequence in memory only. It never reaches storage.
    OperationResult replaceDerivedSequence(
        const ProjectId& project_id,
        const SequenceId& sequence_id,
        const TimeseriesBatch& data);
    OperationResult replaceDerivedSequence(
        const SequenceId& sequence_id,
        const TimeseriesBatch& data);

    // Replaces only one affected time range of a derived sequence. This is
    // useful when a new source point changes interpolation or a pointwise
    // formula locally; callers must use replaceDerivedSequence() for a full
    // rebuild or when the affected range is not known safely.
    OperationResult patchDerivedSequence(
        const ProjectId& project_id,
        const SequenceId& sequence_id,
        Timestamp start_time,
        Timestamp end_time,
        const TimeseriesBatch& data);
    OperationResult patchDerivedSequence(
        const SequenceId& sequence_id,
        Timestamp start_time,
        Timestamp end_time,
        const TimeseriesBatch& data);

    WindowQueryResult queryWindowData(
        const ProjectId& project_id,
        const WindowQuery& query) const;
    WindowQueryResult queryWindowData(const WindowQuery& query) const;
    WindowStatisticsResult queryWindowStatistics(
        const ProjectId& project_id,
        const std::vector<SequenceId>& sequence_ids) const;
    WindowStatisticsResult queryWindowStatistics(
        const std::vector<SequenceId>& sequence_ids) const;

private:
    struct SequenceExecutor;

    // Keep a small side buffer for rare corrections. Once it reaches this
    // bound, merge it into the ordered vector in one batch instead of moving
    // the whole live vector for every single out-of-order point.
    static constexpr std::size_t kLatePointFlushThreshold = 64;

    struct SequenceWindow {
        mutable std::shared_mutex mutex;
        std::vector<RawTimeseriesPoint> points;
        // Rare late/corrected points are kept separately so one correction
        // does not shift the whole append-oriented vector. Queries merge this
        // ordered side buffer with points on demand.
        std::map<Timestamp, RawTimeseriesPoint> late_points;
        std::size_t active_begin{0};
        std::optional<Timestamp> latest_time;
        // Ordered appends and left-edge eviction maintain these in O(1)
        // amortized time. Rare corrections invalidate the cache and trigger
        // one rebuild when statistics are next requested.
        bool statistics_valid{true};
        std::size_t statistics_count{0};
        std::size_t statistics_non_numeric_count{0};
        long double statistics_sum{0.0L};
        std::deque<std::pair<Timestamp, double>> statistics_minimum;
        std::deque<std::pair<Timestamp, double>> statistics_maximum;

        bool empty() const {
            return active_begin >= points.size() && late_points.empty();
        }
    };

    static void replaceSequence(
        SequenceWindow& sequence,
        const TimeseriesBatch& data);
    static void mergeSortedPoints(
        SequenceWindow& sequence,
        std::vector<RawTimeseriesPoint> incoming);
    static void flushLatePoints(SequenceWindow& sequence);
    static void compactSequence(SequenceWindow& sequence);
    static void resetStatistics(SequenceWindow& sequence);
    static void appendStatistic(
        SequenceWindow& sequence,
        const RawTimeseriesPoint& point);
    static void removeStatistic(
        SequenceWindow& sequence,
        const RawTimeseriesPoint& point);
    static void rebuildStatistics(SequenceWindow& sequence);

    OperationResult updateWindow(
        const ProjectId& project_id,
        const TimeseriesBatch& data,
        std::optional<std::int64_t> window_size_override);
    OperationResult updateWindow(
        const TimeseriesBatch& data,
        std::optional<std::int64_t> window_size_override);
    WindowUpdateResult updateWindowIncremental(
        const ProjectId& project_id,
        const TimeseriesBatch& data,
        std::optional<std::int64_t> window_size_override);
    bool pruneExpiredPoints(
        const ProjectId& project_id,
        Timestamp window_start,
        const std::vector<std::pair<SequenceId, std::shared_ptr<SequenceWindow>>>&
            sequences,
        std::unordered_set<SequenceId>* evicted_sequence_ids = nullptr,
        double* lock_wait_ms = nullptr,
        double* update_ms = nullptr);

    std::shared_ptr<SequenceWindow> sequenceWindowFor(
        const ProjectId& project_id,
        const SequenceId& sequence_id);

    // Protects the hot-window index, watermark and window-size metadata from
    // concurrent ingest and query RPCs.
    // Shared queries do not block each other. Writers take this unique lock
    // only to update the index, watermark and eviction snapshot; the actual
    // point insertion and pruning use independent sequence locks.
    mutable std::shared_mutex mutex_;
    std::unordered_map<SequenceId, std::shared_ptr<SequenceWindow>>
        sequence_windows_;
    std::unordered_map<ProjectId, Timestamp> watermarks_;
    std::unordered_map<ProjectId, std::int64_t> window_sizes_;
    std::unordered_map<ProjectId, std::uint64_t> update_generations_;
    std::unique_ptr<SequenceExecutor> sequence_executor_;
};

}  // namespace sfkg::timeseries::core
