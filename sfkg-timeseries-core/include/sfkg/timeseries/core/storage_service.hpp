#pragma once

#include "sfkg/timeseries/core/types.hpp"
#include "sfkg/timeseries/core/internal/taos_client.hpp"

namespace sfkg::timeseries::core {

class StorageService {
public:
    explicit StorageService(internal::TaosClient& taos_client)
        : taos_client_(taos_client) {}

    OperationResult writeRawData(
        const ProjectId& project_id,
        const TimeseriesBatch& data);
    OperationResult writeRawData(const TimeseriesBatch& data);
    OperationResult writeRawDataOnConnection(
        const ProjectId& project_id,
        std::size_t connection_index,
        const TimeseriesBatch& data);
    OperationResult writeRawDataOnConnection(
        std::size_t connection_index,
        const TimeseriesBatch& data);

private:
    internal::TaosClient& taos_client_;
};

}  // namespace sfkg::timeseries::core
