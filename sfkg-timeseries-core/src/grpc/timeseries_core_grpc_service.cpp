#include "sfkg/timeseries/core/grpc/timeseries_core_grpc_service.hpp"

#include <algorithm>
#include <chrono>
#include <exception>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
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

OperationResult unavailable(
    std::size_t failed_count,
    std::string message) {
    return internal::makeOperationResult(
        OperationCode::Unavailable,
        0,
        failed_count,
        std::move(message));
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
    const OperationResult& window,
    const OperationResult& derived) {
    if (!isSuccessful(resolve.code)) {
        return resolve;
    }
    const bool storage_succeeded = isSuccessful(storage.code);
    const bool window_succeeded = isSuccessful(window.code);
    const bool derived_succeeded = isSuccessful(derived.code);

    if (!storage_succeeded && !window_succeeded && !derived_succeeded) {
        return internal::makeOperationResult(
            storage.code,
            0,
            resolve.failed_count + resolve.success_count,
            "cold storage, hot window and derived refresh failed; cold: " +
                storage.message + "; hot: " + window.message +
                "; derived: " + derived.message);
    }
    if (!storage_succeeded) {
        return internal::makeOperationResult(
            OperationCode::PartialSuccess,
            window.success_count + derived.success_count,
            resolve.failed_count + storage.failed_count + derived.failed_count,
            "hot window/derived refresh completed but cold storage failed: " +
                storage.message);
    }
    if (!window_succeeded) {
        return internal::makeOperationResult(
            OperationCode::PartialSuccess,
            storage.success_count,
            resolve.failed_count + window.failed_count + derived.failed_count,
            "cold storage succeeded but hot window update failed; derived "
            "refresh was skipped: " + window.message);
    }
    if (!derived_succeeded) {
        return internal::makeOperationResult(
            OperationCode::PartialSuccess,
            storage.success_count + window.success_count,
            resolve.failed_count + derived.failed_count,
            "cold storage and hot window updated but derived refresh failed: " +
                derived.message);
    }
    if (resolve.code == OperationCode::PartialSuccess ||
        storage.code == OperationCode::PartialSuccess ||
        window.code == OperationCode::PartialSuccess ||
        derived.code == OperationCode::PartialSuccess) {
        return internal::makeOperationResult(
            OperationCode::PartialSuccess,
            resolve.success_count,
            resolve.failed_count + storage.failed_count + window.failed_count,
            "cold storage, hot window and derived refresh completed with "
            "partial success");
    }
    return internal::ok(
        resolve.success_count,
        "ingest data stored, hot window updated and derived windows refreshed");
}

bool validTimeRange(
    const std::optional<Timestamp>& start,
    const std::optional<Timestamp>& end) {
    return !start || !end || *start <= *end;
}

std::vector<SequenceId> mappedSequenceIds(const ConstraintRule& rule) {
    std::vector<SequenceId> sequence_ids;
    sequence_ids.reserve(rule.variable_mapping.size());
    for (const auto& [variable, sequence_id] : rule.variable_mapping) {
        (void)variable;
        if (!sequence_id.empty()) {
            sequence_ids.push_back(sequence_id);
        }
    }
    std::sort(sequence_ids.begin(), sequence_ids.end());
    sequence_ids.erase(
        std::unique(sequence_ids.begin(), sequence_ids.end()),
        sequence_ids.end());
    return sequence_ids;
}

bool containsAllSequences(
    const WindowData& data,
    const std::vector<SequenceId>& sequence_ids) {
    for (const auto& sequence_id : sequence_ids) {
        if (data.sequence_values.find(sequence_id) ==
            data.sequence_values.end()) {
            return false;
        }
    }
    return true;
}

bool hasTwoDistinctTimestamps(const WindowData& data) {
    std::optional<Timestamp> first_time;
    for (const auto& [sequence_id, points] : data.sequence_values) {
        (void)sequence_id;
        for (const auto& point : points) {
            if (!first_time) {
                first_time = point.time;
            } else if (point.time != *first_time) {
                return true;
            }
        }
    }
    return false;
}

template <typename Value>
void appendUnique(std::vector<Value>* values, const Value& value) {
    if (std::find(values->begin(), values->end(), value) == values->end()) {
        values->push_back(value);
    }
}

ConstraintCheckResult runContinuousConstraintCheck(
    const WindowData& window_data,
    const std::vector<ConstraintRule>& enabled_rules,
    const AlignmentService& alignment_service,
    const ConstraintCheckEngine& constraint_engine) {
    ConstraintCheckResult result;
    result.satisfied = true;
    result.operation = internal::ok(
        0, "constraint check skipped: no applicable enabled rules");
    if (enabled_rules.empty()) {
        return result;
    }

    std::vector<ConstraintRule> single_sequence_rules;
    std::vector<ConstraintRule> multi_sequence_rules;
    for (const auto& rule : enabled_rules) {
        const auto sequence_ids = mappedSequenceIds(rule);
        // A rule whose sequences have not appeared in the current hot window
        // is not an error during continuous ingest; it becomes checkable once
        // the missing sequence data arrives.
        if (sequence_ids.empty() ||
            !containsAllSequences(window_data, sequence_ids)) {
            continue;
        }
        if (sequence_ids.size() == 1) {
            single_sequence_rules.push_back(rule);
        } else {
            multi_sequence_rules.push_back(rule);
        }
    }

    if (!single_sequence_rules.empty()) {
        const auto single_result = constraint_engine.checkConstraints(
            single_sequence_rules, window_data);
        if (!isSuccessful(single_result.operation.code)) {
            return single_result;
        }
        result.evaluated_count += single_result.evaluated_count;
        result.violations.insert(
            result.violations.end(),
            single_result.violations.begin(),
            single_result.violations.end());
    }

    if (!multi_sequence_rules.empty()) {
        // Multi-sequence constraints use ordinary time alignment only.
        // Relation configs and lag adjustments are deliberately unrelated to
        // constraint checking.
        if (!hasTwoDistinctTimestamps(window_data)) {
            result.satisfied = result.violations.empty();
            result.operation = internal::ok(
                result.evaluated_count,
                result.satisfied
                    ? "constraint check skipped: insufficient timestamps "
                      "for ordinary alignment"
                    : "single-sequence violations found; multi-sequence "
                      "check skipped due to insufficient timestamps");
            return result;
        }
        const auto alignment = alignment_service.alignWindowData(window_data);
        if (!isSuccessful(alignment.operation.code)) {
            ConstraintCheckResult failed;
            failed.operation = alignment.operation;
            failed.satisfied = false;
            return failed;
        }
        const auto multi_result = constraint_engine.checkConstraints(
            multi_sequence_rules, alignment.aligned_data);
        if (!isSuccessful(multi_result.operation.code)) {
            return multi_result;
        }
        result.evaluated_count += multi_result.evaluated_count;
        result.violations.insert(
            result.violations.end(),
            multi_result.violations.begin(),
            multi_result.violations.end());
    }

    result.satisfied = result.violations.empty();
    result.operation = internal::ok(
        result.evaluated_count,
        result.satisfied
            ? "continuous constraint checks completed; all satisfied"
            : "continuous constraint checks completed; violations found");
    return result;
}

OperationResult withConstraintNotificationFailure(
    OperationResult base,
    const OperationResult& notification) {
    if (isSuccessful(notification.code)) {
        return base;
    }
    if (base.code == OperationCode::Ok) {
        base.code = OperationCode::PartialSuccess;
    }
    if (!base.message.empty()) {
        base.message += "; ";
    }
    base.message += "constraint check/notification failed: " +
        notification.message;
    return base;
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

::grpc::Status TimeseriesCoreGrpcService::syncDerivedSeriesConfigs(
    ::grpc::ServerContext* context,
    const pb::SyncDerivedSeriesConfigsRequest* request,
    pb::SyncConfigResponse* response) {
    (void)context;
    return guardedCall("syncDerivedSeriesConfigs", response, [&] {
        RuntimeConfigSnapshot<RuntimeDerivedSeriesConfig> snapshot;
        snapshot.items.reserve(request->items_size());
        std::string error;
        for (const auto& item : request->items()) {
            RuntimeDerivedSeriesConfig converted;
            if (!conversion::fromProto(item, &converted, &error)) {
                conversion::toProto(
                    internal::invalidArgument(error),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
            snapshot.items.push_back(std::move(converted));
        }

        const auto configured =
            config_registry_.upsertDerivedSeriesConfigs(snapshot);
        if (!isSuccessful(configured.code)) {
            conversion::toProto(configured, response->mutable_operation());
            return ::grpc::Status::OK;
        }
        const auto refreshed = derived_series_service_.refresh();
        if (!isSuccessful(refreshed.code)) {
            auto result = refreshed;
            if (!result.message.empty()) {
                result.message = "derived configuration synchronized, but " +
                    result.message;
            }
            conversion::toProto(result, response->mutable_operation());
            return ::grpc::Status::OK;
        }
        conversion::toProto(configured, response->mutable_operation());
        return ::grpc::Status::OK;
    });
}

IngestPipelineResult TimeseriesCoreGrpcService::processHotIngest(
    const TimeseriesBatch& data) {
    IngestPipelineResult result;
    result.window_result = window_service_.buildTimeWindow(data);
    if (isSuccessful(result.window_result.code)) {
        result.derived_result = derived_series_service_.refresh();
    } else {
        result.derived_result = failedPrecondition(
            "derived refresh skipped because hot window update failed");
    }

    result.constraint_notification_result = internal::ok(
        0, "constraint check skipped because hot window update failed");
    if (!isSuccessful(result.window_result.code)) {
        return result;
    }

    const auto enabled_rules = config_registry_.allEnabledConstraints();
    if (enabled_rules.empty()) {
        result.constraint_notification_result = internal::ok(
            0, "no enabled constraints; notification skipped");
        return result;
    }

    std::vector<SequenceId> requested_sequence_ids;
    std::unordered_set<SequenceId> requested_sequence_set;
    for (const auto& rule : enabled_rules) {
        for (const auto& sequence_id : mappedSequenceIds(rule)) {
            if (requested_sequence_set.insert(sequence_id).second) {
                requested_sequence_ids.push_back(sequence_id);
            }
        }
    }

    if (requested_sequence_ids.empty()) {
        result.constraint_notification_result = internal::invalidArgument(
            "enabled constraints contain no mapped sequences");
        return result;
    }

    const auto window = window_service_.queryWindowData({
        requested_sequence_ids, std::nullopt, std::nullopt});
    if (!isSuccessful(window.operation.code)) {
        result.constraint_notification_result = window.operation;
        return result;
    }
    if (window.data.sequence_values.empty()) {
        result.constraint_notification_result = internal::ok(
            0,
            "constraint check skipped because hot window has no applicable data");
        return result;
    }

    const auto check = runContinuousConstraintCheck(
        window.data,
        enabled_rules,
        alignment_service_,
        constraint_engine_);
    if (!isSuccessful(check.operation.code)) {
        result.constraint_notification_result = check.operation;
        return result;
    }
    if (check.violations.empty()) {
        result.constraint_notification_result = internal::ok(
            check.evaluated_count,
            "no constraint violations; notification skipped");
        return result;
    }

    std::unordered_map<std::string, std::vector<SequenceId>>
        sequences_by_constraint;
    for (const auto& rule : enabled_rules) {
        sequences_by_constraint[rule.constraint_id] = mappedSequenceIds(rule);
    }

    std::vector<std::string> violated_constraint_ids;
    std::vector<SequenceId> violated_sequence_ids;
    for (const auto& violation : check.violations) {
        appendUnique(&violated_constraint_ids, violation.constraint_id);
        const auto found = sequences_by_constraint.find(
            violation.constraint_id);
        if (found != sequences_by_constraint.end()) {
            for (const auto& sequence_id : found->second) {
                appendUnique(&violated_sequence_ids, sequence_id);
            }
        }
    }
    std::sort(
        violated_constraint_ids.begin(), violated_constraint_ids.end());
    std::sort(violated_sequence_ids.begin(), violated_sequence_ids.end());
    result.constraint_notification_result =
        constraint_result_receiver_.receiveConstraintResult(
            window.data.window_end_time,
            violated_constraint_ids,
            violated_sequence_ids);
    return result;
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
            conversion::toProto(invalid, response->mutable_derived_result());
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
                conversion::toProto(
                    invalid, response->mutable_derived_result());
                return ::grpc::Status::OK;
            }
            input.push_back(std::move(converted));
        }

        auto resolved = std::make_shared<IngestResult>(
            ingest_service_.ingestAndResolveData(input));
        conversion::toProto(
            resolved->operation, response->mutable_resolve_result());

        OperationResult storage_result;
        OperationResult window_result;
        OperationResult derived_result;
        OperationResult constraint_notification_result = internal::ok(
            0, "constraint check skipped because hot window update failed");
        OperationResult ingest_result;
        if (isSuccessful(resolved->operation.code)) {
            auto submission = ingest_task_executor_.trySubmit(
                resolved,
                [this](const TimeseriesBatch& data) {
                    return storage_service_.writeRawData(data);
                },
                [this](const TimeseriesBatch& data) {
                    return processHotIngest(data);
                });
            if (!submission.accepted) {
                const auto failed_count = resolved->resolved_data.points.size();
                storage_result = unavailable(
                    failed_count, submission.admission.message);
                window_result = unavailable(
                    failed_count, submission.admission.message);
                derived_result = unavailable(
                    failed_count, "derived refresh skipped: ingest task was not admitted");
                constraint_notification_result = internal::ok(
                    0, "constraint check skipped: ingest task was not admitted");
                ingest_result = unavailable(
                    resolved->operation.success_count +
                        resolved->operation.failed_count,
                    submission.admission.message);
            } else {
                const auto pipeline = submission.completion.get();
                storage_result = pipeline.storage_result;
                window_result = pipeline.window_result;
                derived_result = pipeline.derived_result;
                constraint_notification_result =
                    pipeline.constraint_notification_result;
                ingest_result = combineIngestResults(
                    resolved->operation,
                    storage_result,
                    window_result,
                    derived_result);
            }
        } else {
            storage_result = failedPrecondition(
                "storage skipped because ingest resolution did not succeed");
            window_result = failedPrecondition(
                "window update skipped because ingest resolution did not succeed");
            derived_result = failedPrecondition(
                "derived refresh skipped because ingest resolution did not succeed");
            ingest_result = combineIngestResults(
                resolved->operation,
                storage_result,
                window_result,
                derived_result);
        }

        conversion::toProto(
            storage_result, response->mutable_storage_result());
        conversion::toProto(
            window_result, response->mutable_window_result());
        conversion::toProto(
            derived_result, response->mutable_derived_result());

        conversion::toProto(
            constraint_notification_result,
            response->mutable_constraint_notification_result());
        ingest_result = withConstraintNotificationFailure(
            std::move(ingest_result), constraint_notification_result);
        conversion::toProto(ingest_result, response->mutable_operation());
        if (request->return_resolved_data()) {
            conversion::toProto(
                resolved->resolved_data, response->mutable_resolved_data());
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
