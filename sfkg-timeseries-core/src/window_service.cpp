#include "sfkg/timeseries/core/window_service.hpp"

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core {

OperationResult WindowService::buildTimeWindow(
    const TimeseriesBatch& data,
    std::int64_t window_size) {
    (void)window_size;
    return internal::notImplemented(
        "buildTimeWindow", data.points.size());
}

WindowQueryResult WindowService::queryWindowData(
    const WindowQuery& query) const {
    WindowQueryResult result;
    result.operation = internal::notImplemented(
        "queryWindowData", query.sequence_ids.size());
    return result;
}

}  // namespace sfkg::timeseries::core
