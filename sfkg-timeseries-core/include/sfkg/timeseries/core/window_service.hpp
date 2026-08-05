#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <unordered_map>

#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core {

class WindowService {
public:
    OperationResult buildTimeWindow(
        const TimeseriesBatch& data,
        std::int64_t window_size);

    WindowQueryResult queryWindowData(
        const WindowQuery& query) const;

private:
    using SequenceWindow = std::map<Timestamp, RawTimeseriesPoint>;

    // Protects the hot-window index, watermark and window-size metadata from
    // concurrent ingest and query RPCs.
    mutable std::mutex mutex_;
    std::unordered_map<SequenceId, SequenceWindow> sequence_windows_;
    std::optional<Timestamp> watermark_;
    std::int64_t window_size_{0};
};

}  // namespace sfkg::timeseries::core
