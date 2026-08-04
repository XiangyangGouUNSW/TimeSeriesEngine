#include "sfkg/timeseries/core/alignment_service.hpp"

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core {

AlignmentResult AlignmentService::alignWindowData(
    const WindowData& window_data,
    const AlignmentConfig& config) const {
    (void)window_data;
    AlignmentResult result;
    result.operation = internal::notImplemented(
        "alignWindowData", config.sequences.size());
    return result;
}

}  // namespace sfkg::timeseries::core
