#pragma once

#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core {

class StatisticsService {
public:
    StatisticsResult computeBasicStatistics(
        const WindowData& data) const;
    StatisticsResult computeBasicStatistics(
        const AlignedWindowData& data,
        const AlignmentConfig& config) const;
};

}  // namespace sfkg::timeseries::core
