#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <unordered_map>

#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core {

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

    // Replaces a derived sequence in memory only. It never reaches storage.
    OperationResult replaceDerivedSequence(
        const SequenceId& sequence_id,
        const TimeseriesBatch& data);

    WindowQueryResult queryWindowData(
        const WindowQuery& query) const;

private:
    using SequenceWindow = std::map<Timestamp, RawTimeseriesPoint>;

    OperationResult updateWindow(
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
