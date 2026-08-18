#pragma once

#include <optional>

#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core {

struct ConstraintCheckRange {
    Timestamp start_time{};
    Timestamp end_time{};
};

class ConstraintCheckEngine {
public:
    ConstraintCheckResult checkConstraints(
        const ProjectId& project_id,
        const std::vector<ConstraintRule>& rules,
        const WindowData& data) const;
    ConstraintCheckResult checkConstraints(
        const std::vector<ConstraintRule>& rules,
        const WindowData& data) const;
    ConstraintCheckResult checkConstraints(
        const ProjectId& project_id,
        const std::vector<ConstraintRule>& rules,
        const AlignedWindowData& data) const;
    ConstraintCheckResult checkConstraints(
        const std::vector<ConstraintRule>& rules,
        const AlignedWindowData& data) const;
    ConstraintCheckResult checkConstraints(
        const ProjectId& project_id,
        const std::vector<ConstraintRule>& rules,
        const WindowData& data,
        const std::optional<ConstraintCheckRange>& range) const;
    ConstraintCheckResult checkConstraints(
        const std::vector<ConstraintRule>& rules,
        const WindowData& data,
        const std::optional<ConstraintCheckRange>& range) const;
    ConstraintCheckResult checkConstraints(
        const ProjectId& project_id,
        const std::vector<ConstraintRule>& rules,
        const AlignedWindowData& data,
        const std::optional<ConstraintCheckRange>& range) const;
    ConstraintCheckResult checkConstraints(
        const std::vector<ConstraintRule>& rules,
        const AlignedWindowData& data,
        const std::optional<ConstraintCheckRange>& range) const;
};

}  // namespace sfkg::timeseries::core
