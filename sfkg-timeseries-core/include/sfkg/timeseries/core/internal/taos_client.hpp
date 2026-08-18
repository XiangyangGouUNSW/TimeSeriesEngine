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

    OperationResult ensureSchema() const;
    OperationResult ensureProjectSchema(const ProjectId& project_id) const;
    // Explicitly scoped for local tests and demos; normal services must not
    // drop their configured database as part of ordinary shutdown.
    OperationResult dropDatabaseForTesting();
    OperationResult insertRaw(
        const ProjectId& project_id,
        const TimeseriesBatch& batch);
    OperationResult insertRaw(const TimeseriesBatch& batch);
    // Used by the fixed writer shards. The shard index is mapped to one
    // connection and is not selected from the first point in the batch.
    OperationResult insertRawOnConnection(
        const ProjectId& project_id,
        std::size_t connection_index,
        const TimeseriesBatch& batch);
    OperationResult insertRawOnConnection(
        std::size_t connection_index,
        const TimeseriesBatch& batch);
    OperationResult queryRaw(
        const ProjectId& project_id,
        const std::vector<SequenceId>& sequence_ids,
        Timestamp start,
        Timestamp end,
        TimeseriesBatch* out,
        std::optional<std::int64_t> granularity = std::nullopt,
        const std::unordered_map<SequenceId, TimeseriesValueKind>* value_kinds =
            nullptr) const;
    OperationResult queryRaw(
        const std::vector<SequenceId>& sequence_ids,
        Timestamp start,
        Timestamp end,
        TimeseriesBatch* out,
        std::optional<std::int64_t> granularity = std::nullopt,
        const std::unordered_map<SequenceId, TimeseriesValueKind>* value_kinds =
            nullptr) const;
    OperationResult queryHistoryOverview(
        const ProjectId& project_id,
        const HistoryOverviewQuery& query,
        HistoryOverview* out) const;
    OperationResult queryHistoryOverview(
        const HistoryOverviewQuery& query,
        HistoryOverview* out) const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace sfkg::timeseries::core::internal
