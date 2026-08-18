#include "sfkg/timeseries/core/storage_service.hpp"

namespace sfkg::timeseries::core {

OperationResult StorageService::writeRawData(
    const ProjectId& project_id,
    const TimeseriesBatch& data) {
    return taos_client_.insertRaw(project_id, data);
}

OperationResult StorageService::writeRawData(const TimeseriesBatch& data) {
    return writeRawData(data.project_id.empty() ? ProjectId{"default"}
                                                : data.project_id,
                        data);
}

OperationResult StorageService::writeRawDataOnConnection(
    const ProjectId& project_id,
    std::size_t connection_index,
    const TimeseriesBatch& data) {
    return taos_client_.insertRawOnConnection(project_id, connection_index, data);
}

OperationResult StorageService::writeRawDataOnConnection(
    std::size_t connection_index,
    const TimeseriesBatch& data) {
    return writeRawDataOnConnection(
        data.project_id.empty() ? ProjectId{"default"} : data.project_id,
        connection_index,
        data);
}

}  // namespace sfkg::timeseries::core
