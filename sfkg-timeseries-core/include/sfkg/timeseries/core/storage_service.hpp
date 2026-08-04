#pragma once

#include "sfkg/timeseries/core/types.hpp"
#include "sfkg/timeseries/core/internal/taos_client.hpp"

namespace sfkg::timeseries::core {

class StorageService {
public:
    explicit StorageService(internal::TaosClient& taos_client)
        : taos_client_(taos_client) {}

    OperationResult writeRawData(const TimeseriesBatch& data);

private:
    internal::TaosClient& taos_client_;
};

}  // namespace sfkg::timeseries::core
