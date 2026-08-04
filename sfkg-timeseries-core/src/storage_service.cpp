#include "sfkg/timeseries/core/storage_service.hpp"

namespace sfkg::timeseries::core {

OperationResult StorageService::writeRawData(const TimeseriesBatch& data) {
    return taos_client_.insertRaw(data);
}

}  // namespace sfkg::timeseries::core
