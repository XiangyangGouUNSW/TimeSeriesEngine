#pragma once

#include "sfkg/timeseries/core/runtime_config_registry.hpp"

namespace sfkg::timeseries::core {

class IngestService {
public:
    explicit IngestService(const RuntimeConfigRegistry& configs)
        : configs_(configs) {}

    IngestResult ingestAndResolveData(
        const ProjectId& project_id,
        const std::vector<TimeseriesIngestData>& input) const;
    IngestResult ingestAndResolveData(
        const std::vector<TimeseriesIngestData>& input) const;

private:
    const RuntimeConfigRegistry& configs_;
};

}  // namespace sfkg::timeseries::core
