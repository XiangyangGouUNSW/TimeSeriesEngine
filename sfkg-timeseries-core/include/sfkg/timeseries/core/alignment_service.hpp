#pragma once

#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core {

class RuntimeConfigRegistry;

class AlignmentService {
public:
    explicit AlignmentService(const RuntimeConfigRegistry& configs)
        : configs_(configs) {}

    // Ordinary alignment with defaults resolved from the local registry.
    AlignmentResult alignWindowData(
        const WindowData& window_data) const;

    // Ordinary alignment with optional per-sequence strategies and interval.
    AlignmentResult alignWindowData(
        const WindowData& window_data,
        const AlignmentConfig& config) const;

    // Relation-aware alignment with all other settings resolved by Core.
    AlignmentResult alignWindowData(
        const WindowData& window_data,
        const std::vector<RuntimeRelationConfig>& relations) const;

    // Relation-aware alignment. An empty relation list is ordinary alignment.
    AlignmentResult alignWindowData(
        const WindowData& window_data,
        const AlignmentConfig& config,
        const std::vector<RuntimeRelationConfig>& relations) const;

private:
    const RuntimeConfigRegistry& configs_;
};

}  // namespace sfkg::timeseries::core
