#pragma once

#include <memory>
#include <string>
#include <vector>

#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core::internal {

class TaosClient {
public:
    TaosClient();
    ~TaosClient();

    TaosClient(const TaosClient&) = delete;
    TaosClient& operator=(const TaosClient&) = delete;

    OperationResult ensureSchema();
    // Explicitly scoped for local tests and demos; normal services must not
    // drop their configured database as part of ordinary shutdown.
    OperationResult dropDatabaseForTesting();
    OperationResult insertRaw(const TimeseriesBatch& batch);
    OperationResult queryRaw(
        const std::vector<SequenceId>& sequence_ids,
        Timestamp start,
        Timestamp end,
        TimeseriesBatch* out) const;
    OperationResult queryHistoryOverview(
        const HistoryOverviewQuery& query,
        HistoryOverview* out) const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace sfkg::timeseries::core::internal
