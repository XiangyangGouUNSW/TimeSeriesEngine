#pragma once

#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core {

class WindowService {
public:
    OperationResult buildTimeWindow(
        const TimeseriesBatch& data,
        std::int64_t window_size);

    WindowQueryResult queryWindowData(
        const WindowQuery& query) const;
};

}  // namespace sfkg::timeseries::core
