#include "sfkg/timeseries/core/history_query_service.hpp"

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core {

HistoryQueryResult HistoryQueryService::queryHistoryData(
    const HistoryQuery& query) const {
    HistoryQueryResult result;
    if (query.sequence_ids.empty() || query.start_time > query.end_time) {
        result.operation = internal::invalidArgument(
            "sequence_ids must not be empty and time range must be ordered");
        return result;
    }
    for (const auto& sequence_id : query.sequence_ids) {
        if (sequence_id.empty()) {
            result.operation = internal::invalidArgument(
                "sequence_id must not be empty");
            return result;
        }
        if (!configs_.findInstance(sequence_id)) {
            result.operation = internal::makeOperationResult(
                OperationCode::NotFound, 0, 0,
                "sequence is not registered: " + sequence_id);
            return result;
        }
    }
    result.operation = taos_client_.queryRaw(
        query.sequence_ids, query.start_time, query.end_time, &result.data);
    return result;
}

HistoryOverviewResult HistoryQueryService::queryHistoryOverview(
    const HistoryOverviewQuery& query) const {
    HistoryOverviewResult result;
    if (query.start_time && query.end_time &&
        *query.start_time > *query.end_time) {
        result.operation = internal::invalidArgument(
            "overview start_time must not be after end_time");
        return result;
    }
    for (const auto& sequence_id : query.sequence_ids) {
        if (sequence_id.empty()) {
            result.operation = internal::invalidArgument(
                "sequence_id must not be empty");
            return result;
        }
        if (!configs_.findInstance(sequence_id)) {
            result.operation = internal::makeOperationResult(
                OperationCode::NotFound, 0, 0,
                "sequence is not registered: " + sequence_id);
            return result;
        }
    }
    result.operation = taos_client_.queryHistoryOverview(
        query, &result.overview);
    return result;
}

}  // namespace sfkg::timeseries::core
