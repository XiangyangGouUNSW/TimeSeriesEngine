#pragma once

#include "sfkg/timeseries/core/runtime_config_registry.hpp"
#include "sfkg/timeseries/core/internal/taos_client.hpp"

namespace sfkg::timeseries::core {

class HistoryQueryService {
public:
    HistoryQueryService(
        const RuntimeConfigRegistry& configs,
        internal::TaosClient& taos_client)
        : configs_(configs), taos_client_(taos_client) {}

    HistoryQueryResult queryHistoryData(
        const ProjectId& project_id,
        const HistoryQuery& query) const;
    HistoryQueryResult queryHistoryData(const HistoryQuery& query) const;
    HistoryOverviewResult queryHistoryOverview(
        const ProjectId& project_id,
        const HistoryOverviewQuery& query) const;
    HistoryOverviewResult queryHistoryOverview(
        const HistoryOverviewQuery& query) const;

private:
    const RuntimeConfigRegistry& configs_;
    internal::TaosClient& taos_client_;
};

}  // namespace sfkg::timeseries::core
