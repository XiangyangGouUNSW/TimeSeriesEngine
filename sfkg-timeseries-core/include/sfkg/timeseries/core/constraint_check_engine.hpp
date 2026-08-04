#pragma once

#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core {

class ConstraintCheckEngine {
public:
    ConstraintCheckResult checkConstraints(
        const std::vector<ConstraintRule>& rules,
        const WindowData& data) const;
    ConstraintCheckResult checkConstraints(
        const std::vector<ConstraintRule>& rules,
        const AlignedWindowData& data) const;
};

}  // namespace sfkg::timeseries::core
