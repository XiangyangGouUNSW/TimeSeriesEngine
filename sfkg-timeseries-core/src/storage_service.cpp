#include "sfkg/timeseries/core/storage_service.hpp"

namespace sfkg::timeseries::core {

OperationResult StorageService::writeRawData(const TimeseriesBatch& data) {
    return taos_client_.insertRaw(data);
}

OperationResult StorageService::writeRawDataOnConnection(
    std::size_t connection_index,
    const TimeseriesBatch& data) {
    return taos_client_.insertRawOnConnection(connection_index, data);
}

}  // namespace sfkg::timeseries::core
