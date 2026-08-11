#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <unordered_map>

#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core {

// Describes the part of the hot window affected by one raw-data update.
// affected_end_time is inclusive. Incremental consumers must fall back to a
// full refresh when incremental_safe is false, for example after an
// out-of-order write or when the sliding window advances and evicts data.
struct WindowUpdateResult {
    OperationResult operation;
    std::vector<SequenceId> changed_sequence_ids;
    std::optional<Timestamp> affected_start_time;
    std::optional<Timestamp> affected_end_time;
    bool incremental_safe{false};
};

class WindowService {
public:
    static constexpr std::int64_t kDefaultWindowSizeMs =
        ::sfkg::timeseries::core::kDefaultWindowSizeMs;

    OperationResult configureWindowSize(std::int64_t window_size);
    std::int64_t windowSize() const;

    // Uses the runtime-configured window size.
    OperationResult buildTimeWindow(const TimeseriesBatch& data);

    // Explicit size is retained for fine-grained tests, replay and
    // compensation flows.
    OperationResult buildTimeWindow(
        const TimeseriesBatch& data,
        std::int64_t window_size);

    // Same hot-window update as buildTimeWindow(), with change information
    // for incremental derived-series and constraint processing.
    WindowUpdateResult buildTimeWindowIncremental(
        const TimeseriesBatch& data);

    // Replaces a derived sequence in memory only. It never reaches storage.
    OperationResult replaceDerivedSequence(
        const SequenceId& sequence_id,
        const TimeseriesBatch& data);

    // Replaces only one affected time range of a derived sequence. This is
    // useful when a new source point changes interpolation or a pointwise
    // formula locally; callers must use replaceDerivedSequence() for a full
    // rebuild or when the affected range is not known safely.
    OperationResult patchDerivedSequence(
        const SequenceId& sequence_id,
        Timestamp start_time,
        Timestamp end_time,
        const TimeseriesBatch& data);

    WindowQueryResult queryWindowData(
        const WindowQuery& query) const;

private:
    using SequenceWindow = std::map<Timestamp, RawTimeseriesPoint>;

    OperationResult updateWindow(
        const TimeseriesBatch& data,
        std::optional<std::int64_t> window_size_override);
    WindowUpdateResult updateWindowIncremental(
        const TimeseriesBatch& data,
        std::optional<std::int64_t> window_size_override);
    void pruneExpiredPoints();

    // Protects the hot-window index, watermark and window-size metadata from
    // concurrent ingest and query RPCs.
    mutable std::mutex mutex_;
    std::unordered_map<SequenceId, SequenceWindow> sequence_windows_;
    std::optional<Timestamp> watermark_;
    std::int64_t window_size_{kDefaultWindowSizeMs};
};

}  // namespace sfkg::timeseries::core
