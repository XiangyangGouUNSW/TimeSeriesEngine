#include "sfkg/timeseries/core/history_query_service.hpp"

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core {
namespace {

TimeseriesValueKind valueKindForDataType(const std::string& data_type) {
    if (data_type == "double" || data_type == "float" ||
        data_type == "continuous") {
        return TimeseriesValueKind::Double;
    }
    if (data_type == "int" || data_type == "int64" ||
        data_type == "integer" || data_type == "discrete") {
        return TimeseriesValueKind::Int64;
    }
    if (data_type == "bool" || data_type == "boolean") {
        return TimeseriesValueKind::Bool;
    }
    if (data_type == "string" || data_type == "text" ||
        data_type == "label" || data_type == "categorical") {
        return TimeseriesValueKind::String;
    }
    return TimeseriesValueKind::Unknown;
}

}  // namespace

HistoryQueryResult HistoryQueryService::queryHistoryData(
    const ProjectId& project_id,
    const HistoryQuery& query) const {
    HistoryQueryResult result;
    result.project_id = project_id;
    result.data.project_id = project_id;
    std::unordered_map<SequenceId, TimeseriesValueKind> value_kinds;
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
        const auto config = configs_.findInstance(project_id, sequence_id);
        if (!config) {
            result.operation = internal::makeOperationResult(
                OperationCode::NotFound, 0, 0,
                "sequence is not registered: " + sequence_id);
            return result;
        }
        value_kinds.emplace(
            sequence_id, valueKindForDataType(config->data_type));
    }
    result.operation = taos_client_.queryRaw(
        project_id,
        query.sequence_ids,
        query.start_time,
        query.end_time,
        &result.data,
        query.granularity,
        &value_kinds);
    return result;
}

HistoryQueryResult HistoryQueryService::queryHistoryData(
    const HistoryQuery& query) const {
    return queryHistoryData(
        query.project_id.empty() ? ProjectId{"default"} : query.project_id,
        query);
}

HistoryOverviewResult HistoryQueryService::queryHistoryOverview(
    const ProjectId& project_id,
    const HistoryOverviewQuery& query) const {
    HistoryOverviewResult result;
    result.project_id = project_id;
    result.overview.project_id = project_id;
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
        if (!configs_.findInstance(project_id, sequence_id)) {
            result.operation = internal::makeOperationResult(
                OperationCode::NotFound, 0, 0,
                "sequence is not registered: " + sequence_id);
            return result;
        }
    }
    result.operation = taos_client_.queryHistoryOverview(
        project_id,
        query, &result.overview);
    return result;
}

HistoryOverviewResult HistoryQueryService::queryHistoryOverview(
    const HistoryOverviewQuery& query) const {
    return queryHistoryOverview(
        query.project_id.empty() ? ProjectId{"default"} : query.project_id,
        query);
}

}  // namespace sfkg::timeseries::core
