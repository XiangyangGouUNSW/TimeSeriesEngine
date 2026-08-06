#pragma once

#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core {

class AlignmentService {
public:
    AlignmentResult alignWindowData(
        const WindowData& window_data,
        const AlignmentConfig& config,
        const std::vector<RuntimeRelationConfig>& relations) const;
};

}  // namespace sfkg::timeseries::core
