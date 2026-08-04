#include "sfkg/timeseries/core/constraint_check_engine.hpp"

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core {

ConstraintCheckResult ConstraintCheckEngine::checkConstraints(
    const std::vector<ConstraintRule>& rules,
    const WindowData& data) const {
    (void)data;
    ConstraintCheckResult result;
    result.operation = internal::notImplemented(
        "checkConstraints(WindowData)", rules.size());
    return result;
}

ConstraintCheckResult ConstraintCheckEngine::checkConstraints(
    const std::vector<ConstraintRule>& rules,
    const AlignedWindowData& data) const {
    (void)data;
    ConstraintCheckResult result;
    result.operation = internal::notImplemented(
        "checkConstraints(AlignedWindowData)", rules.size());
    return result;
}

}  // namespace sfkg::timeseries::core
