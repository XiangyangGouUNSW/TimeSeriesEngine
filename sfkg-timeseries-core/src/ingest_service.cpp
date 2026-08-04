#include "sfkg/timeseries/core/ingest_service.hpp"

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core {

IngestResult IngestService::ingestAndResolveData(
    const std::vector<TimeseriesIngestData>& input) const {
    IngestResult result;
    result.operation = internal::notImplemented(
        "ingestAndResolveData", input.size());
    return result;
}

}  // namespace sfkg::timeseries::core
