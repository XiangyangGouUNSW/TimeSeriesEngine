#include "sfkg/timeseries/core/statistics_service.hpp"

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core {

StatisticsResult StatisticsService::computeBasicStatistics(
    const WindowData& data) const {
    StatisticsResult result;
    result.operation = internal::notImplemented(
        "computeBasicStatistics(WindowData)", data.sequence_values.size());
    return result;
}

StatisticsResult StatisticsService::computeBasicStatistics(
    const AlignedWindowData& data,
    const AlignmentConfig& config) const {
    (void)data;
    StatisticsResult result;
    result.operation = internal::notImplemented(
        "computeBasicStatistics(AlignedWindowData)",
        config.sequences.size());
    return result;
}

}  // namespace sfkg::timeseries::core
