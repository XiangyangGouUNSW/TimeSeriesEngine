#include "sfkg/timeseries/core/grpc/timeseries_core_grpc_service.hpp"

#include <chrono>
#include <exception>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "grpc/internal/proto_conversion.hpp"
#include "operation_helpers.hpp"

namespace sfkg::timeseries::core::grpc {
namespace {

OperationResult failedPrecondition(std::string message) {
    return internal::makeOperationResult(
        OperationCode::FailedPrecondition, 0, 0, std::move(message));
}

OperationResult internalError(std::string message) {
    return internal::makeOperationResult(
        OperationCode::InternalError, 0, 0, std::move(message));
}

std::string joinConstraintIds(const std::vector<std::string>& ids) {
    std::ostringstream output;
    for (std::size_t index = 0; index < ids.size(); ++index) {
        if (index != 0) {
            output << ", ";
        }
        output << ids[index];
    }
    return output.str();
}

template <typename Response, typename Function>
::grpc::Status guardedCall(
    const char* rpc_name,
    Response* response,
    Function&& function) {
    const auto started = std::chrono::steady_clock::now();
    try {
        ::grpc::Status status = function();
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started);
        std::clog << "rpc=" << rpc_name
                  << " grpc_code=" << status.error_code()
                  << " elapsed_ms=" << elapsed.count() << '\n';
        return status;
    } catch (const std::exception& exception) {
        conversion::toProto(
            internalError(exception.what()), response->mutable_operation());
        std::cerr << "rpc=" << rpc_name
                  << " unhandled_exception=" << exception.what() << '\n';
        return {::grpc::StatusCode::INTERNAL, "internal server error"};
    } catch (...) {
        conversion::toProto(
            internalError("unknown internal exception"),
            response->mutable_operation());
        std::cerr << "rpc=" << rpc_name
                  << " unhandled_exception=unknown\n";
        return {::grpc::StatusCode::INTERNAL, "internal server error"};
    }
}

bool isSuccessful(OperationCode code) {
    return code == OperationCode::Ok ||
           code == OperationCode::PartialSuccess;
}

OperationResult combineIngestResults(
    const OperationResult& resolve,
    const OperationResult& storage,
    const OperationResult& window) {
    if (!isSuccessful(resolve.code)) {
        return resolve;
    }
    const bool storage_succeeded = isSuccessful(storage.code);
    const bool window_succeeded = isSuccessful(window.code);

    if (!storage_succeeded && !window_succeeded) {
        return internal::makeOperationResult(
            storage.code,
            0,
            resolve.failed_count + resolve.success_count,
            "cold storage and hot window writes failed; cold: " +
                storage.message + "; hot: " + window.message);
    }
    if (!storage_succeeded) {
        return internal::makeOperationResult(
            OperationCode::PartialSuccess,
            window.success_count,
            resolve.failed_count + storage.failed_count,
            "hot window updated but cold storage failed: " +
                storage.message);
    }
    if (!window_succeeded) {
        return internal::makeOperationResult(
            OperationCode::PartialSuccess,
            storage.success_count,
            resolve.failed_count + window.failed_count,
            "cold storage succeeded but hot window update failed: " +
                window.message);
    }
    if (resolve.code == OperationCode::PartialSuccess ||
        storage.code == OperationCode::PartialSuccess ||
        window.code == OperationCode::PartialSuccess) {
        return internal::makeOperationResult(
            OperationCode::PartialSuccess,
            resolve.success_count,
            resolve.failed_count + storage.failed_count + window.failed_count,
            "cold storage and hot window writes completed with partial "
            "success");
    }
    return internal::ok(
        resolve.success_count, "ingest data stored and hot window updated");
}

bool validTimeRange(
    const std::optional<Timestamp>& start,
    const std::optional<Timestamp>& end) {
    return !start || !end || *start <= *end;
}

}  // namespace

::grpc::Status TimeseriesCoreGrpcService::syncInstanceConfigs(
    ::grpc::ServerContext* context,
    const pb::SyncInstanceConfigsRequest* request,
    pb::SyncConfigResponse* response) {
    (void)context;
    return guardedCall("syncInstanceConfigs", response, [&] {
        RuntimeConfigSnapshot<RuntimeInstanceConfig> snapshot;
        snapshot.items.reserve(request->items_size());
        std::string error;
        for (const auto& item : request->items()) {
            RuntimeInstanceConfig converted;
            if (!conversion::fromProto(item, &converted, &error)) {
                conversion::toProto(
                    internal::invalidArgument(error),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
            snapshot.items.push_back(std::move(converted));
        }
        conversion::toProto(
            config_registry_.upsertInstanceConfigs(snapshot),
            response->mutable_operation());
        return ::grpc::Status::OK;
    });
}

::grpc::Status TimeseriesCoreGrpcService::syncConstraints(
    ::grpc::ServerContext* context,
    const pb::SyncConstraintsRequest* request,
    pb::SyncConfigResponse* response) {
    (void)context;
    return guardedCall("syncConstraints", response, [&] {
        RuntimeConfigSnapshot<RuntimeConstraintConfig> snapshot;
        snapshot.items.reserve(request->items_size());
        std::string error;
        for (const auto& item : request->items()) {
            RuntimeConstraintConfig converted;
            if (!conversion::fromProto(item, &converted, &error)) {
                conversion::toProto(
                    internal::invalidArgument(error),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
            snapshot.items.push_back(std::move(converted));
        }
        conversion::toProto(
            config_registry_.upsertConstraints(snapshot),
            response->mutable_operation());
        return ::grpc::Status::OK;
    });
}

::grpc::Status TimeseriesCoreGrpcService::syncRelations(
    ::grpc::ServerContext* context,
    const pb::SyncRelationsRequest* request,
    pb::SyncConfigResponse* response) {
    (void)context;
    return guardedCall("syncRelations", response, [&] {
        RuntimeConfigSnapshot<RuntimeRelationConfig> snapshot;
        snapshot.items.reserve(request->items_size());
        std::string error;
        for (const auto& item : request->items()) {
            RuntimeRelationConfig converted;
            if (!conversion::fromProto(item, &converted, &error)) {
                conversion::toProto(
                    internal::invalidArgument(error),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
            snapshot.items.push_back(std::move(converted));
        }
        conversion::toProto(
            config_registry_.upsertRelations(snapshot),
            response->mutable_operation());
        return ::grpc::Status::OK;
    });
}

::grpc::Status TimeseriesCoreGrpcService::syncWindowConfig(
    ::grpc::ServerContext* context,
    const pb::SyncWindowConfigRequest* request,
    pb::SyncConfigResponse* response) {
    (void)context;
    return guardedCall("syncWindowConfig", response, [&] {
        if (!request->has_config()) {
            conversion::toProto(
                internal::invalidArgument("window config is required"),
                response->mutable_operation());
            return ::grpc::Status::OK;
        }
        RuntimeWindowConfig config;
        std::string error;
        if (!conversion::fromProto(
                request->config(), &config, &error)) {
            conversion::toProto(
                internal::invalidArgument(error),
                response->mutable_operation());
            return ::grpc::Status::OK;
        }
        conversion::toProto(
            window_service_.configureWindowSize(config.window_size),
            response->mutable_operation());
        return ::grpc::Status::OK;
    });
}

::grpc::Status TimeseriesCoreGrpcService::ingestData(
    ::grpc::ServerContext* context,
    const pb::IngestDataRequest* request,
    pb::IngestDataResponse* response) {
    (void)context;
    return guardedCall("ingestData", response, [&] {
        if (request->points().empty()) {
            const auto invalid = internal::invalidArgument(
                "points must not be empty");
            conversion::toProto(invalid, response->mutable_operation());
            conversion::toProto(invalid, response->mutable_resolve_result());
            return ::grpc::Status::OK;
        }

        std::vector<TimeseriesIngestData> input;
        input.reserve(request->points_size());
        std::string error;
        for (const auto& point : request->points()) {
            TimeseriesIngestData converted;
            if (!conversion::fromProto(point, &converted, &error)) {
                const auto invalid = internal::invalidArgument(error);
                conversion::toProto(invalid, response->mutable_operation());
                conversion::toProto(
                    invalid, response->mutable_resolve_result());
                return ::grpc::Status::OK;
            }
            input.push_back(std::move(converted));
        }

        const IngestResult resolved =
            ingest_service_.ingestAndResolveData(input);
        conversion::toProto(
            resolved.operation, response->mutable_resolve_result());

        OperationResult storage_result;
        OperationResult window_result;
        if (isSuccessful(resolved.operation.code)) {
            // First version: attempt both destinations independently in the
            // request thread. This is a logical dual write, not an atomic
            // transaction: one destination may succeed while the other fails.
            // Future work can move these calls to independent workers or a
            // write queue, add batch/request IDs for idempotent retries, and
            // add compensation so cold storage remains the rebuildable source
            // of truth for the hot window.
            storage_result = storage_service_.writeRawData(resolved.resolved_data);
            window_result = window_service_.buildTimeWindow(
                resolved.resolved_data);
        } else {
            storage_result = failedPrecondition(
                "storage skipped because ingest resolution did not succeed");
            window_result = failedPrecondition(
                "window update skipped because ingest resolution did not succeed");
        }

        conversion::toProto(
            storage_result, response->mutable_storage_result());
        conversion::toProto(
            window_result, response->mutable_window_result());
        conversion::toProto(
            combineIngestResults(
                resolved.operation, storage_result, window_result),
            response->mutable_operation());
        if (request->return_resolved_data()) {
            conversion::toProto(
                resolved.resolved_data, response->mutable_resolved_data());
        }
        return ::grpc::Status::OK;
    });
}

::grpc::Status TimeseriesCoreGrpcService::ingestAndResolveData(
    ::grpc::ServerContext* context,
    const pb::IngestRequest* request,
    pb::IngestResponse* response) {
    (void)context;
    return guardedCall("ingestAndResolveData", response, [&] {
        if (request->points().empty()) {
            conversion::toProto(
                internal::invalidArgument("points must not be empty"),
                response->mutable_operation());
            return ::grpc::Status::OK;
        }
        std::vector<TimeseriesIngestData> input;
        input.reserve(request->points_size());
        std::string error;
        for (const auto& point : request->points()) {
            TimeseriesIngestData converted;
            if (!conversion::fromProto(point, &converted, &error)) {
                conversion::toProto(
                    internal::invalidArgument(error),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
            input.push_back(std::move(converted));
        }
        const auto result = ingest_service_.ingestAndResolveData(input);
        conversion::toProto(result.operation, response->mutable_operation());
        conversion::toProto(
            result.resolved_data, response->mutable_resolved_data());
        return ::grpc::Status::OK;
    });
}

::grpc::Status TimeseriesCoreGrpcService::writeRawData(
    ::grpc::ServerContext* context,
    const pb::WriteRawDataRequest* request,
    pb::WriteRawDataResponse* response) {
    (void)context;
    return guardedCall("writeRawData", response, [&] {
        if (!request->has_data() || request->data().points().empty()) {
            conversion::toProto(
                internal::invalidArgument("data points must not be empty"),
                response->mutable_operation());
            return ::grpc::Status::OK;
        }
        TimeseriesBatch data;
        std::string error;
        if (!conversion::fromProto(request->data(), &data, &error)) {
            conversion::toProto(
                internal::invalidArgument(error),
                response->mutable_operation());
            return ::grpc::Status::OK;
        }
        conversion::toProto(
            storage_service_.writeRawData(data),
            response->mutable_operation());
        return ::grpc::Status::OK;
    });
}

::grpc::Status TimeseriesCoreGrpcService::buildTimeWindow(
    ::grpc::ServerContext* context,
    const pb::BuildTimeWindowRequest* request,
    pb::BuildTimeWindowResponse* response) {
    (void)context;
    return guardedCall("buildTimeWindow", response, [&] {
        if (!request->has_data() || request->data().points().empty() ||
            request->window_size() <= 0) {
            conversion::toProto(
                internal::invalidArgument(
                    "data points must not be empty and window_size must be positive"),
                response->mutable_operation());
            return ::grpc::Status::OK;
        }
        TimeseriesBatch data;
        std::string error;
        if (!conversion::fromProto(request->data(), &data, &error)) {
            conversion::toProto(
                internal::invalidArgument(error),
                response->mutable_operation());
            return ::grpc::Status::OK;
        }
        conversion::toProto(
            window_service_.buildTimeWindow(data, request->window_size()),
            response->mutable_operation());
        return ::grpc::Status::OK;
    });
}

::grpc::Status TimeseriesCoreGrpcService::queryWindowData(
    ::grpc::ServerContext* context,
    const pb::QueryWindowDataRequest* request,
    pb::QueryWindowDataResponse* response) {
    (void)context;
    return guardedCall("queryWindowData", response, [&] {
        WindowQuery query = conversion::fromProto(*request);
        if (query.sequence_ids.empty() ||
            !validTimeRange(query.start_time, query.end_time)) {
            conversion::toProto(
                internal::invalidArgument(
                    "sequence_ids must not be empty and time range must be ordered"),
                response->mutable_operation());
            return ::grpc::Status::OK;
        }
        const auto result = window_service_.queryWindowData(query);
        conversion::toProto(result.operation, response->mutable_operation());
        conversion::toProto(result.data, response->mutable_data());
        return ::grpc::Status::OK;
    });
}

::grpc::Status TimeseriesCoreGrpcService::alignWindowData(
    ::grpc::ServerContext* context,
    const pb::AlignWindowDataRequest* request,
    pb::AlignWindowDataResponse* response) {
    (void)context;
    return guardedCall("alignWindowData", response, [&] {
        AlignmentConfig config;
        std::string error;
        if (request->has_config() &&
            !conversion::fromProto(request->config(), &config, &error)) {
            conversion::toProto(
                internal::invalidArgument(
                    error.empty() ? "alignment config is invalid" : error),
                response->mutable_operation());
            return ::grpc::Status::OK;
        }

        WindowData data;
        if (request->source_case() == pb::AlignWindowDataRequest::kData) {
            if (!conversion::fromProto(request->data(), &data, &error)) {
                conversion::toProto(
                    internal::invalidArgument(error),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
        } else if (request->source_case() ==
                   pb::AlignWindowDataRequest::kWindowQuery) {
            const auto window_result = window_service_.queryWindowData(
                conversion::fromProto(request->window_query()));
            if (!isSuccessful(window_result.operation.code)) {
                conversion::toProto(
                    window_result.operation, response->mutable_operation());
                return ::grpc::Status::OK;
            }
            data = window_result.data;
        } else {
            conversion::toProto(
                internal::invalidArgument("alignment source is required"),
                response->mutable_operation());
            return ::grpc::Status::OK;
        }

        std::vector<RuntimeRelationConfig> relations;
        relations.reserve(request->relation_ids_size());
        for (const auto& relation_id : request->relation_ids()) {
            if (relation_id.empty()) {
                conversion::toProto(
                    internal::invalidArgument(
                        "alignment relation_id must not be empty"),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
            const auto relation = config_registry_.findRelation(relation_id);
            if (!relation) {
                conversion::toProto(
                    internal::makeOperationResult(
                        OperationCode::NotFound,
                        0,
                        0,
                        "relation is not registered: " + relation_id),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
            if (!relation->enabled) {
                conversion::toProto(
                    failedPrecondition(
                        "relation is not enabled: " + relation_id),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
            relations.push_back(*relation);
        }

        const auto result = alignment_service_.alignWindowData(
            data, config, relations);
        conversion::toProto(result.operation, response->mutable_operation());
        conversion::toProto(
            result.aligned_data, response->mutable_aligned_data());
        return ::grpc::Status::OK;
    });
}

::grpc::Status TimeseriesCoreGrpcService::computeBasicStatistics(
    ::grpc::ServerContext* context,
    const pb::ComputeStatisticsRequest* request,
    pb::ComputeStatisticsResponse* response) {
    (void)context;
    return guardedCall("computeBasicStatistics", response, [&] {
        std::string error;
        if (request->source_case() ==
            pb::ComputeStatisticsRequest::kWindowData) {
            if (!request->relation_id().empty()) {
                conversion::toProto(
                    internal::invalidArgument(
                        "relation_id requires aligned_data as the statistics source"),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
            WindowData data;
            if (!conversion::fromProto(request->window_data(), &data, &error)) {
                conversion::toProto(
                    internal::invalidArgument(error),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
            conversion::toProto(
                statistics_service_.computeBasicStatistics(data), response);
            return ::grpc::Status::OK;
        }

        if (request->source_case() ==
            pb::ComputeStatisticsRequest::kAlignedData) {
            AlignedWindowData data;
            if (!conversion::fromProto(
                    request->aligned_data(), &data, &error)) {
                conversion::toProto(
                    internal::invalidArgument(
                        error.empty() ? "aligned data is required" : error),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }

            if (request->relation_id().empty()) {
                conversion::toProto(
                    internal::invalidArgument(
                        "relation_id is required for aligned statistics"),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
            const auto relation =
                config_registry_.findRelation(request->relation_id());
            if (!relation) {
                conversion::toProto(
                    internal::makeOperationResult(
                        OperationCode::NotFound,
                        0,
                        0,
                        "relation is not registered: " + request->relation_id()),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
            if (!relation->enabled) {
                conversion::toProto(
                    failedPrecondition(
                        "relation is not enabled: " + request->relation_id()),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
            conversion::toProto(
                statistics_service_.computeBasicStatistics(data, *relation),
                response);
            return ::grpc::Status::OK;
        }

        if (request->source_case() ==
            pb::ComputeStatisticsRequest::kWindowQuery) {
            if (!request->relation_id().empty()) {
                conversion::toProto(
                    internal::invalidArgument(
                        "relation_id requires aligned_data as the statistics source"),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
            const auto window_result = window_service_.queryWindowData(
                conversion::fromProto(request->window_query()));
            if (!isSuccessful(window_result.operation.code)) {
                conversion::toProto(
                    window_result.operation, response->mutable_operation());
                return ::grpc::Status::OK;
            }
            conversion::toProto(
                statistics_service_.computeBasicStatistics(window_result.data),
                response);
            return ::grpc::Status::OK;
        }

        conversion::toProto(
            internal::invalidArgument("statistics source is required"),
            response->mutable_operation());
        return ::grpc::Status::OK;
    });
}

::grpc::Status TimeseriesCoreGrpcService::checkConstraints(
    ::grpc::ServerContext* context,
    const pb::CheckConstraintsRequest* request,
    pb::CheckConstraintsResponse* response) {
    (void)context;
    return guardedCall("checkConstraints", response, [&] {
        if (request->constraint_ids().empty()) {
            conversion::toProto(
                internal::invalidArgument("constraint_ids must not be empty"),
                response->mutable_operation());
            return ::grpc::Status::OK;
        }
        const std::vector<std::string> constraint_ids(
            request->constraint_ids().begin(), request->constraint_ids().end());
        const auto lookup = config_registry_.lookupConstraints(constraint_ids);
        if (!lookup.missing_ids.empty() || !lookup.disabled_ids.empty()) {
            std::ostringstream message;
            message << "requested constraints are not all enabled";
            if (!lookup.missing_ids.empty()) {
                message << "; missing=["
                        << joinConstraintIds(lookup.missing_ids) << "]";
            }
            if (!lookup.disabled_ids.empty()) {
                message << "; disabled=["
                        << joinConstraintIds(lookup.disabled_ids) << "]";
            }
            conversion::toProto(
                failedPrecondition(message.str()),
                response->mutable_operation());
            return ::grpc::Status::OK;
        }
        const auto& rules = lookup.enabled_rules;

        std::string error;
        if (request->source_case() ==
            pb::CheckConstraintsRequest::kWindowData) {
            WindowData data;
            if (!conversion::fromProto(request->window_data(), &data, &error)) {
                conversion::toProto(
                    internal::invalidArgument(error),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
            conversion::toProto(
                constraint_engine_.checkConstraints(rules, data), response);
            return ::grpc::Status::OK;
        }

        if (request->source_case() ==
            pb::CheckConstraintsRequest::kAlignedData) {
            AlignedWindowData data;
            if (!conversion::fromProto(
                    request->aligned_data(), &data, &error)) {
                conversion::toProto(
                    internal::invalidArgument(error),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
            conversion::toProto(
                constraint_engine_.checkConstraints(rules, data), response);
            return ::grpc::Status::OK;
        }

        if (request->source_case() ==
            pb::CheckConstraintsRequest::kWindowQuery) {
            const auto window_result = window_service_.queryWindowData(
                conversion::fromProto(request->window_query()));
            if (!isSuccessful(window_result.operation.code)) {
                conversion::toProto(
                    window_result.operation, response->mutable_operation());
                return ::grpc::Status::OK;
            }
            conversion::toProto(
                constraint_engine_.checkConstraints(
                    rules, window_result.data),
                response);
            return ::grpc::Status::OK;
        }

        conversion::toProto(
            internal::invalidArgument("constraint input source is required"),
            response->mutable_operation());
        return ::grpc::Status::OK;
    });
}

::grpc::Status TimeseriesCoreGrpcService::queryHistoryData(
    ::grpc::ServerContext* context,
    const pb::QueryHistoryDataRequest* request,
    pb::QueryHistoryDataResponse* response) {
    (void)context;
    return guardedCall("queryHistoryData", response, [&] {
        const HistoryQuery query = conversion::fromProto(*request);
        if (query.sequence_ids.empty() || query.start_time > query.end_time ||
            (query.granularity && *query.granularity <= 0)) {
            conversion::toProto(
                internal::invalidArgument(
                    "history query sequence_ids, range or granularity is invalid"),
                response->mutable_operation());
            return ::grpc::Status::OK;
        }
        const auto result = history_service_.queryHistoryData(query);
        conversion::toProto(result.operation, response->mutable_operation());
        conversion::toProto(result.data, response->mutable_data());
        return ::grpc::Status::OK;
    });
}

::grpc::Status TimeseriesCoreGrpcService::queryHistoryOverview(
    ::grpc::ServerContext* context,
    const pb::QueryHistoryOverviewRequest* request,
    pb::QueryHistoryOverviewResponse* response) {
    (void)context;
    return guardedCall("queryHistoryOverview", response, [&] {
        const HistoryOverviewQuery query =
            conversion::fromProto(*request);
        if (query.start_time && query.end_time &&
            *query.start_time > *query.end_time) {
            conversion::toProto(
                internal::invalidArgument(
                    "history overview time range is invalid"),
                response->mutable_operation());
            return ::grpc::Status::OK;
        }
        const auto result = history_service_.queryHistoryOverview(query);
        conversion::toProto(result, response);
        return ::grpc::Status::OK;
    });
}

}  // namespace sfkg::timeseries::core::grpc
