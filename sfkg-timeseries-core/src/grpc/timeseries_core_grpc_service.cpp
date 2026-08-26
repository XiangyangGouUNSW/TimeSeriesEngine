#include "sfkg/timeseries/core/grpc/timeseries_core_grpc_service.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <future>
#include <iostream>
#include <limits>
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

struct TimeseriesCoreGrpcService::IngestDiagnostics {
    using Clock = std::chrono::steady_clock;

    struct WriterTiming {
        std::size_t shard_count{0};
        std::size_t point_count{0};
        double write_sum_ms{0.0};
        double write_max_ms{0.0};
    };

    std::mutex mutex;
    Clock::time_point submitted{};
    Clock::time_point first_lane_start{};
    Clock::time_point first_cold_start{};
    Clock::time_point last_cold_end{};
    Clock::time_point hot_start{};
    Clock::time_point hot_end{};
    bool has_first_lane_start{false};
    bool has_cold_start{false};
    bool has_cold_end{false};
    std::size_t request_points{0};
    std::size_t resolved_points{0};
    std::size_t cold_shard_count{0};
    double resolve_ms{0.0};
    double hot_window_ms{0.0};
    double config_snapshot_ms{0.0};
    double constraint_prepare_ms{0.0};
    double constraint_group_span_ms{0.0};
    double window_sequence_lock_wait_ms{0.0};
    double window_sequence_update_ms{0.0};
    double window_eviction_lock_wait_ms{0.0};
    double window_eviction_update_ms{0.0};
    double derived_ms{0.0};
    double constraint_query_ms{0.0};
    double constraint_check_ms{0.0};
    double constraint_notify_ms{0.0};
    bool window_incremental_safe{false};
    bool window_evicted{false};
    std::size_t window_sequence_count{0};
    std::size_t window_incremental_safe_sequence_count{0};
    std::size_t window_sequence_task_count{0};
    std::size_t window_sequence_group_count{0};
    std::unordered_map<std::size_t, WriterTiming> writers;
};

namespace {

bool diagnosticFlagEnabled(const char* name) {
    const char* configured = std::getenv(name);
    return configured != nullptr &&
        (std::string(configured) == "1" ||
         std::string(configured) == "true" ||
         std::string(configured) == "on");
}

std::size_t diagnosticSampleEvery() {
    const char* configured = std::getenv(
        "SFKG_INGEST_DIAGNOSTIC_SAMPLE_EVERY");
    if (configured == nullptr || *configured == '\0') {
        return 1;
    }
    std::size_t value = 0;
    for (const char* cursor = configured; *cursor != '\0'; ++cursor) {
        if (*cursor < '0' || *cursor > '9') {
            return 1;
        }
        const auto digit = static_cast<std::size_t>(*cursor - '0');
        if (value > (std::numeric_limits<std::size_t>::max() - digit) / 10) {
            return 1;
        }
        value = value * 10 + digit;
    }
    return value == 0 ? 1 : value;
}

bool shouldCollectIngestDiagnostics() {
    static const bool enabled = diagnosticFlagEnabled(
        "SFKG_INGEST_DIAGNOSTIC_LOG");
    static const auto sample_every = diagnosticSampleEvery();
    static std::atomic<std::uint64_t> request_counter{0};
    if (!enabled) {
        return false;
    }
    const auto request_number = request_counter.fetch_add(
        1, std::memory_order_relaxed) + 1;
    return request_number % sample_every == 0;
}

const char* operationCodeName(OperationCode code) {
    switch (code) {
        case OperationCode::Ok:
            return "OK";
        case OperationCode::PartialSuccess:
            return "PARTIAL_SUCCESS";
        case OperationCode::InvalidArgument:
            return "INVALID_ARGUMENT";
        case OperationCode::NotFound:
            return "NOT_FOUND";
        case OperationCode::FailedPrecondition:
            return "FAILED_PRECONDITION";
        case OperationCode::Unavailable:
            return "UNAVAILABLE";
        case OperationCode::InternalError:
            return "INTERNAL_ERROR";
        case OperationCode::NotImplemented:
            return "NOT_IMPLEMENTED";
    }
    return "UNKNOWN";
}

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

ProjectId projectFor(const std::string& project_id) {
    // Empty is retained as the wire-compatible single-project default. New
    // clients should always send a non-empty project_id; the default keeps
    // older replay tools readable while still isolating all explicit IDs.
    return project_id.empty() ? ProjectId{"default"} : project_id;
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

using ConstraintSequenceCache =
    std::unordered_map<std::string, std::vector<SequenceId>>;

struct ConstraintIncrementalRange {
    Timestamp start_time{0};
    Timestamp end_time{0};
    bool window_evicted{false};
};

std::optional<ConstraintIncrementalRange> incrementalRangeFor(
    const WindowUpdateResult* update,
    const std::vector<SequenceId>& sequence_ids,
    const std::unordered_set<SequenceId>* changed_sequence_set = nullptr) {
    if (update == nullptr || sequence_ids.empty()) {
        return std::nullopt;
    }
    if (update->sequence_updates.empty()) {
        if (!update->incremental_safe ||
            !update->affected_start_time ||
            !update->affected_end_time) {
            return std::nullopt;
        }
        ConstraintIncrementalRange range{
            *update->affected_start_time,
            *update->affected_end_time,
            update->window_evicted};
        if (range.window_evicted && update->window_start_time) {
            range.start_time = std::min(
                range.start_time, *update->window_start_time);
        }
        return range;
    }

    std::optional<ConstraintIncrementalRange> range;
    for (const auto& sequence_id : sequence_ids) {
        const bool changed = changed_sequence_set != nullptr
            ? changed_sequence_set->find(sequence_id) !=
                changed_sequence_set->end()
            : std::find(
                  update->changed_sequence_ids.begin(),
                  update->changed_sequence_ids.end(),
                  sequence_id) != update->changed_sequence_ids.end();
        if (!changed) {
            // Unchanged rule inputs remain query context; only changed
            // dependencies determine whether this rule can be incremental.
            continue;
        }
        const auto found = update->sequence_updates.find(sequence_id);
        if (found == update->sequence_updates.end() ||
            !found->second.incremental_safe ||
            !found->second.affected_start_time ||
            !found->second.affected_end_time) {
            return std::nullopt;
        }
        if (!range) {
            range = ConstraintIncrementalRange{
                *found->second.affected_start_time,
                *found->second.affected_end_time,
                found->second.window_evicted};
        } else {
            range->start_time = std::min(
                range->start_time, *found->second.affected_start_time);
            range->end_time = std::max(
                range->end_time, *found->second.affected_end_time);
            range->window_evicted = range->window_evicted ||
                found->second.window_evicted;
        }
    }
    if (range && range->window_evicted && update->window_start_time) {
        range->start_time = std::min(
            range->start_time, *update->window_start_time);
    }
    return range;
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
    const ProjectId& project_id,
    const WindowData& window_data,
    const std::vector<ConstraintRule>& enabled_rules,
    const AlignmentService& alignment_service,
    const ConstraintCheckEngine& constraint_engine,
    const std::optional<ConstraintIncrementalRange>& incremental_range,
    const ConstraintSequenceCache& sequence_cache) {
    ConstraintCheckResult result;
    result.project_id = project_id;
    result.satisfied = true;
    result.operation = internal::ok(
        0, "constraint check skipped: no applicable enabled rules");
    if (enabled_rules.empty()) {
        return result;
    }

    std::vector<ConstraintRule> single_sequence_rules;
    std::vector<ConstraintRule> multi_sequence_rules;
    for (const auto& rule : enabled_rules) {
        const auto& sequence_ids = sequence_cache.at(rule.constraint_id);
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
        std::optional<ConstraintCheckRange> range;
        if (incremental_range) {
            range = ConstraintCheckRange{
                incremental_range->start_time,
                incremental_range->end_time};
            for (const auto& rule : single_sequence_rules) {
                const auto& sequence_ids =
                    sequence_cache.at(rule.constraint_id);
                if (sequence_ids.size() != 1) {
                    range.reset();
                    break;
                }
                const auto sequence = window_data.sequence_values.find(
                    sequence_ids.front());
                if (sequence == window_data.sequence_values.end()) {
                    continue;
                }
                std::size_t max_offset = 0;
                for (const auto& term : rule.terms) {
                    max_offset = std::max(max_offset, term.sample_offset);
                }
                const auto first = std::lower_bound(
                    sequence->second.begin(), sequence->second.end(),
                    range->start_time,
                    [](const RawTimeseriesPoint& point, Timestamp time) {
                        return point.time < time;
                    });
                auto anchor = first;
                while (anchor != sequence->second.begin() && max_offset != 0) {
                    --anchor;
                    --max_offset;
                }
                if (anchor != sequence->second.end()) {
                    range->start_time = std::min(
                        range->start_time, anchor->time);
                }
            }
        }
        const auto single_result = range
            ? constraint_engine.checkConstraints(
                  project_id,
                  single_sequence_rules, window_data, range)
            : constraint_engine.checkConstraints(
                  project_id,
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
        std::size_t maximum_offset = 0;
        for (const auto& rule : multi_sequence_rules) {
            for (const auto& term : rule.terms) {
                maximum_offset = std::max(maximum_offset, term.sample_offset);
            }
        }
        std::optional<ConstraintCheckRange> range;
        if (incremental_range) {
            range = ConstraintCheckRange{
                incremental_range->start_time,
                incremental_range->end_time};
        }
        const auto alignment = range
            ? alignment_service.alignWindowData(
                  project_id,
                  window_data,
                  AlignmentRange{
                      range->start_time,
                      range->end_time,
                      maximum_offset})
            : alignment_service.alignWindowData(project_id, window_data);
        if (!isSuccessful(alignment.operation.code)) {
            ConstraintCheckResult failed;
            failed.operation = alignment.operation;
            failed.satisfied = false;
            return failed;
        }
        const auto multi_result = range
            ? constraint_engine.checkConstraints(
                  project_id,
                  multi_sequence_rules, alignment.aligned_data, range)
            : constraint_engine.checkConstraints(
                  project_id,
                  multi_sequence_rules, alignment.aligned_data);
        if (!isSuccessful(multi_result.operation.code)) {
            return multi_result;
        }
        result.evaluated_count += multi_result.evaluated_count;
        result.pending_count += multi_result.pending_count;
        result.violations.insert(
            result.violations.end(),
            multi_result.violations.begin(),
            multi_result.violations.end());
    }

    result.satisfied = result.violations.empty();
    std::string message = result.satisfied
        ? "continuous constraint checks completed; all satisfied"
        : "continuous constraint checks completed; violations found";
    if (result.pending_count != 0) {
        message += "; " + std::to_string(result.pending_count) +
            " aligned samples pending";
    }
    result.operation = internal::ok(
        result.evaluated_count,
        std::move(message));
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
        const auto project_id = projectFor(request->project_id());
        response->set_project_id(project_id);
        RuntimeConfigSnapshot<RuntimeInstanceConfig> snapshot;
        snapshot.project_id = project_id;
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
            converted.project_id = project_id;
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
        const auto project_id = projectFor(request->project_id());
        response->set_project_id(project_id);
        RuntimeConfigSnapshot<RuntimeConstraintConfig> snapshot;
        snapshot.project_id = project_id;
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
            converted.project_id = project_id;
            converted.rule.project_id = project_id;
            snapshot.items.push_back(std::move(converted));
        }
        const auto configured =
            config_registry_.upsertConstraints(snapshot);
        if (isSuccessful(configured.code)) {
            std::lock_guard state_lock(constraint_state_mutex_);
            constraint_states_.erase(project_id);
        }
        conversion::toProto(configured, response->mutable_operation());
        return ::grpc::Status::OK;
    });
}

::grpc::Status TimeseriesCoreGrpcService::syncRelations(
    ::grpc::ServerContext* context,
    const pb::SyncRelationsRequest* request,
    pb::SyncConfigResponse* response) {
    (void)context;
    return guardedCall("syncRelations", response, [&] {
        const auto project_id = projectFor(request->project_id());
        response->set_project_id(project_id);
        RuntimeConfigSnapshot<RuntimeRelationConfig> snapshot;
        snapshot.project_id = project_id;
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
            converted.project_id = project_id;
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
        const auto project_id = projectFor(request->project_id());
        response->set_project_id(project_id);
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
        config.project_id = project_id;
        conversion::toProto(
            window_service_.configureWindowSize(project_id, config.window_size),
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
        const auto project_id = projectFor(request->project_id());
        response->set_project_id(project_id);
        RuntimeConfigSnapshot<RuntimeDerivedSeriesConfig> snapshot;
        snapshot.project_id = project_id;
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
            converted.project_id = project_id;
            snapshot.items.push_back(std::move(converted));
        }

        const auto configured =
            config_registry_.upsertDerivedSeriesConfigs(snapshot);
        if (!isSuccessful(configured.code)) {
            conversion::toProto(configured, response->mutable_operation());
            return ::grpc::Status::OK;
        }
        const auto refreshed = derived_series_service_.refresh(project_id);
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
    const ProjectId& project_id,
    const TimeseriesBatch& data,
    const std::shared_ptr<IngestDiagnostics>& diagnostics) {
    IngestPipelineResult result;
    const auto window_started = std::chrono::steady_clock::now();
    const auto window_update =
        window_service_.buildTimeWindowIncremental(project_id, data);
    const auto window_finished = std::chrono::steady_clock::now();
    if (diagnostics) {
        std::lock_guard lock(diagnostics->mutex);
        diagnostics->hot_window_ms =
            std::chrono::duration<double, std::milli>(
                window_finished - window_started).count();
        diagnostics->window_incremental_safe =
            window_update.incremental_safe;
        diagnostics->window_evicted = window_update.window_evicted;
        diagnostics->window_sequence_count =
            window_update.changed_sequence_ids.size();
        diagnostics->window_incremental_safe_sequence_count = 0;
        for (const auto& sequence_id : window_update.changed_sequence_ids) {
            const auto found = window_update.sequence_updates.find(sequence_id);
            if (found != window_update.sequence_updates.end() &&
                found->second.incremental_safe) {
                ++diagnostics->window_incremental_safe_sequence_count;
            }
        }
        diagnostics->window_sequence_lock_wait_ms =
            window_update.sequence_lock_wait_ms;
        diagnostics->window_sequence_update_ms =
            window_update.sequence_update_ms;
        diagnostics->window_sequence_task_count =
            window_update.sequence_task_count;
        diagnostics->window_sequence_group_count =
            window_update.sequence_group_count;
        diagnostics->window_eviction_lock_wait_ms =
            window_update.eviction_lock_wait_ms;
        diagnostics->window_eviction_update_ms =
            window_update.eviction_update_ms;
    }
    result.window_result = window_update.operation;
    const auto constraint_prepare_started = std::chrono::steady_clock::now();
    const auto recordConstraintPrepare = [&] {
        if (!diagnostics) {
            return;
        }
        std::lock_guard lock(diagnostics->mutex);
        diagnostics->constraint_prepare_ms =
            std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() -
                constraint_prepare_started).count();
    };
    std::unordered_set<SequenceId> changed_sequence_set;
    changed_sequence_set.reserve(window_update.changed_sequence_ids.size());
    for (const auto& sequence_id : window_update.changed_sequence_ids) {
        changed_sequence_set.insert(sequence_id);
    }

    const auto config_started = std::chrono::steady_clock::now();
    const auto enabled_rules = config_registry_.allEnabledConstraints(project_id);
    const auto derived_configs = config_registry_.allDerivedSeries(project_id);
    const auto config_finished = std::chrono::steady_clock::now();
    if (diagnostics) {
        std::lock_guard lock(diagnostics->mutex);
        diagnostics->config_snapshot_ms =
            std::chrono::duration<double, std::milli>(
                config_finished - config_started).count();
    }

    ConstraintSequenceCache mapped_sequence_cache;
    mapped_sequence_cache.reserve(enabled_rules.size());
    for (const auto& rule : enabled_rules) {
        mapped_sequence_cache.emplace(
            rule.constraint_id, mappedSequenceIds(rule));
    }
    const auto sequenceIdsFor = [&mapped_sequence_cache](
                                   const ConstraintRule& rule)
        -> const std::vector<SequenceId>& {
        return mapped_sequence_cache.at(rule.constraint_id);
    };

    std::unordered_set<SequenceId> enabled_derived_ids;
    for (const auto& derived : derived_configs) {
        if (derived.enabled) {
            enabled_derived_ids.insert(derived.derived_sequence_id);
        }
    }
    const bool has_enabled_derived = !enabled_derived_ids.empty();
    bool constraints_depend_on_derived = false;
    if (!enabled_rules.empty() && has_enabled_derived) {
        for (const auto& rule : enabled_rules) {
            for (const auto& sequence_id : sequenceIdsFor(rule)) {
                if (enabled_derived_ids.find(sequence_id) !=
                    enabled_derived_ids.end()) {
                    constraints_depend_on_derived = true;
                    break;
                }
            }
            if (constraints_depend_on_derived) {
                break;
            }
        }
    }

    std::future<OperationResult> derived_future;
    const auto derived_started = std::chrono::steady_clock::now();
    bool derived_executed = false;
    if (!isSuccessful(result.window_result.code)) {
        result.derived_result = failedPrecondition(
            "derived refresh skipped because hot window update failed");
    } else if (!has_enabled_derived) {
        // There is no derived output to refresh.  Avoid creating an async
        // task just to scan an empty/disabled derived configuration set.
        result.derived_result = internal::ok(
            0, "no enabled derived series; refresh skipped");
    } else if (!constraints_depend_on_derived) {
        // Constraint rules over raw sequences do not depend on the derived
        // output.  Run both read/compute paths concurrently; the derived
        // sequence is still published through WindowService's per-sequence
        // lock before this request completes.
        derived_executed = true;
        derived_future = std::async(
            std::launch::async,
            [this, project_id, window_update] {
                return derived_series_service_.refresh(project_id, window_update);
            });
    } else {
        derived_executed = true;
        result.derived_result = derived_series_service_.refresh(window_update);
    }

    const auto finishDerived = [&] {
        if (!derived_future.valid()) {
            return;
        }
        result.derived_result = derived_future.get();
        const auto derived_finished = std::chrono::steady_clock::now();
        if (diagnostics) {
            std::lock_guard lock(diagnostics->mutex);
            diagnostics->derived_ms =
                std::chrono::duration<double, std::milli>(
                    derived_finished - derived_started).count();
        }
    };
    if (derived_executed && !derived_future.valid() && diagnostics &&
        isSuccessful(result.window_result.code)) {
        const auto derived_finished = std::chrono::steady_clock::now();
        std::lock_guard lock(diagnostics->mutex);
        diagnostics->derived_ms =
            std::chrono::duration<double, std::milli>(
                derived_finished - derived_started).count();
    }

    result.constraint_notification_result = internal::ok(
        0, "constraint check skipped because hot window update failed");
    if (!isSuccessful(result.window_result.code)) {
        recordConstraintPrepare();
        finishDerived();
        return result;
    }

    if (enabled_rules.empty()) {
        recordConstraintPrepare();
        finishDerived();
        result.constraint_notification_result = internal::ok(
            0, "no enabled constraints; notification skipped");
        return result;
    }

    std::vector<ConstraintRule> single_sequence_rules;
    std::vector<ConstraintRule> multi_sequence_rules;
    std::vector<ConstraintRule> aggregate_rules;
    const auto sequenceWasAffected = [&changed_sequence_set](
                                         const std::vector<SequenceId>& ids) {
        return std::any_of(
            ids.begin(), ids.end(),
            [&changed_sequence_set](const SequenceId& sequence_id) {
                return changed_sequence_set.find(sequence_id) !=
                    changed_sequence_set.end();
            });
    };
    const auto clauseKey = [](const ConstraintRule& rule) {
        return rule.or_group_id.empty()
            ? std::string{"\x1f"} + rule.constraint_id
            : std::string{"\x1e"} + rule.or_group_id;
    };
    std::unordered_set<std::string> affected_clauses;
    for (const auto& rule : enabled_rules) {
        if (sequenceWasAffected(sequenceIdsFor(rule))) {
            affected_clauses.insert(clauseKey(rule));
        }
    }
    std::unordered_set<std::string> initialized_rules;
    std::unordered_set<std::string> violated_rules;
    {
        std::lock_guard state_lock(constraint_state_mutex_);
        const auto project_state = constraint_states_.find(project_id);
        if (project_state != constraint_states_.end()) {
            for (const auto& [constraint_id, state] : project_state->second) {
                if (state.initialized) {
                    initialized_rules.insert(constraint_id);
                }
                if (!state.violations.empty()) {
                    violated_rules.insert(constraint_id);
                }
            }
        }
    }
    for (const auto& rule : enabled_rules) {
        if (violated_rules.find(rule.constraint_id) !=
            violated_rules.end()) {
            // Persistent reporting applies to every ingest request, even
            // when this batch did not touch the violating rule's sequence.
            affected_clauses.insert(clauseKey(rule));
        }
    }
    std::vector<ConstraintRule> selected_rules;
    for (const auto& rule : enabled_rules) {
        const auto& sequence_ids = sequenceIdsFor(rule);
        if (sequence_ids.empty() ||
            affected_clauses.find(clauseKey(rule)) == affected_clauses.end()) {
            continue;
        }
        selected_rules.push_back(rule);
        const bool needs_initialization = initialized_rules.find(
            rule.constraint_id) == initialized_rules.end();
        if (!sequenceWasAffected(sequence_ids) && !needs_initialization) {
            continue;
        }
        const bool aggregate = !rule.terms.empty() &&
            rule.terms.front().aggregation != ConstraintAggregation::Sample;
        if (aggregate) {
            aggregate_rules.push_back(rule);
            continue;
        }
        if (sequence_ids.size() == 1) {
            single_sequence_rules.push_back(rule);
        } else if (sequence_ids.size() > 1) {
            multi_sequence_rules.push_back(rule);
        }
    }

    if (selected_rules.empty()) {
        finishDerived();
        result.constraint_notification_result = internal::ok(
            0,
            "no enabled constraints are affected by this window update; "
            "notification skipped");
        return result;
    }

    struct ConstraintGroupExecution {
        ConstraintCheckResult result;
        std::vector<ConstraintRule> rules;
        std::optional<ConstraintIncrementalRange> range;
        double query_ms{0.0};
        double check_ms{0.0};
    };

    struct ConstraintGroupSpec {
        std::vector<ConstraintRule> rules;
        std::optional<ConstraintIncrementalRange> range;
        WindowQuery query;
    };

    const auto makeGroup = [&sequenceIdsFor, &project_id](
                               std::vector<ConstraintRule> rules,
                               std::optional<ConstraintIncrementalRange> range) {
        ConstraintGroupSpec group;
        group.rules = std::move(rules);
        group.range = range;
        std::unordered_set<SequenceId> seen;
        std::size_t maximum_offset = 0;
        for (const auto& rule : group.rules) {
            for (const auto& sequence_id : sequenceIdsFor(rule)) {
                if (seen.insert(sequence_id).second) {
        group.query.sequence_ids.push_back(sequence_id);
                }
            }
            for (const auto& term : rule.terms) {
                maximum_offset = std::max(maximum_offset, term.sample_offset);
            }
        }
        if (group.range) {
            group.query.project_id = project_id;
            group.query.start_time = group.range->start_time;
            group.query.end_time = group.range->end_time ==
                    std::numeric_limits<Timestamp>::max()
                ? group.range->end_time
                : group.range->end_time + 1;
            // The extra context covers positional offsets and the boundary
            // interpolation needed by the aligned multi-sequence path.
            group.query.preceding_points = maximum_offset + 1;
            group.query.following_points = maximum_offset + 1;
        }
        return group;
    };

    const auto splitRulesBySafety = [&window_update, &sequenceIdsFor,
                                     &changed_sequence_set](
                                          const std::vector<ConstraintRule>& rules) {
        std::vector<ConstraintRule> incremental_rules;
        std::vector<ConstraintRule> full_rules;
        for (const auto& rule : rules) {
            if (incrementalRangeFor(
                    &window_update,
                    sequenceIdsFor(rule),
                    &changed_sequence_set)) {
                incremental_rules.push_back(rule);
            } else {
                full_rules.push_back(rule);
            }
        }
        return std::pair{
            std::move(incremental_rules), std::move(full_rules)};
    };

    std::vector<ConstraintGroupSpec> groups;
    const auto addRuleGroups = [&groups, &window_update, &makeGroup,
                                &splitRulesBySafety, &sequenceIdsFor,
                                &changed_sequence_set](
                                   const std::vector<ConstraintRule>& rules) {
        if (rules.empty()) {
            return;
        }
        auto split = splitRulesBySafety(rules);
        if (!split.first.empty()) {
            std::optional<ConstraintIncrementalRange> range;
            for (const auto& rule : split.first) {
                const auto rule_range = incrementalRangeFor(
                    &window_update,
                    sequenceIdsFor(rule),
                    &changed_sequence_set);
                if (!range) {
                    range = rule_range;
                } else {
                    range->start_time = std::min(
                        range->start_time, rule_range->start_time);
                    range->end_time = std::max(
                        range->end_time, rule_range->end_time);
                    range->window_evicted = range->window_evicted ||
                        rule_range->window_evicted;
                }
            }
            groups.push_back(makeGroup(std::move(split.first), range));
        }
        if (!split.second.empty()) {
            groups.push_back(makeGroup(std::move(split.second), std::nullopt));
        }
    };
    const auto addWithoutMixingOrClauses = [&addRuleGroups](
                                                const auto& rules) {
        std::vector<ConstraintRule> ordinary;
        for (const auto& rule : rules) {
            if (rule.or_group_id.empty()) {
                ordinary.push_back(rule);
            } else {
                addRuleGroups(std::vector<ConstraintRule>{rule});
            }
        }
        addRuleGroups(ordinary);
    };
    addWithoutMixingOrClauses(single_sequence_rules);
    addWithoutMixingOrClauses(multi_sequence_rules);
    recordConstraintPrepare();

    const auto run_group = [this, project_id, &mapped_sequence_cache](
                               ConstraintGroupSpec group) {
        ConstraintGroupExecution execution;
        execution.rules = group.rules;
        execution.range = group.range;
        if (group.rules.empty()) {
            execution.result.satisfied = true;
            execution.result.operation = internal::ok(
                0, "constraint check skipped: no rules in group");
            return execution;
        }
        const auto query_started = std::chrono::steady_clock::now();
        const auto window = window_service_.queryWindowData(
            project_id, group.query);
        const auto query_finished = std::chrono::steady_clock::now();
        execution.query_ms = std::chrono::duration<double, std::milli>(
            query_finished - query_started).count();
        if (!isSuccessful(window.operation.code)) {
            execution.result.satisfied = false;
            execution.result.operation = window.operation;
            return execution;
        }
        if (window.data.sequence_values.empty()) {
            execution.result.satisfied = true;
            execution.result.operation = internal::ok(
                0, "constraint check skipped because group has no data");
            return execution;
        }
        const auto check_started = std::chrono::steady_clock::now();
        execution.result = runContinuousConstraintCheck(
            project_id,
            window.data,
            group.rules,
            alignment_service_,
            constraint_engine_,
            group.range,
            mapped_sequence_cache);
        const auto check_finished = std::chrono::steady_clock::now();
        execution.check_ms = std::chrono::duration<double, std::milli>(
            check_finished - check_started).count();
        return execution;
    };

    ConstraintCheckResult check;
    check.satisfied = true;
    check.operation = internal::ok(
        0, "constraint check skipped: no applicable enabled rules");
    double constraint_query_ms = 0.0;
    double constraint_check_ms = 0.0;
    std::vector<std::future<ConstraintGroupExecution>> group_futures;
    group_futures.reserve(groups.size() + aggregate_rules.size());
    const auto constraint_group_started = std::chrono::steady_clock::now();
    for (auto& group : groups) {
        group_futures.push_back(std::async(
            std::launch::async, run_group, std::move(group)));
    }
    for (const auto& rule : aggregate_rules) {
        group_futures.push_back(std::async(
            std::launch::async,
            [this, project_id, rule, &mapped_sequence_cache] {
                ConstraintGroupExecution execution;
                execution.rules.push_back(rule);
                const auto query_started = std::chrono::steady_clock::now();
                const auto statistics =
                    window_service_.queryWindowStatistics(
                        project_id,
                        mapped_sequence_cache.at(rule.constraint_id));
                const auto query_finished = std::chrono::steady_clock::now();
                execution.query_ms =
                    std::chrono::duration<double, std::milli>(
                        query_finished - query_started).count();
                if (!isSuccessful(statistics.operation.code)) {
                    execution.result.operation = statistics.operation;
                    execution.result.satisfied = false;
                    return execution;
                }
                const auto check_started = std::chrono::steady_clock::now();
                execution.result = constraint_engine_.checkConstraints(
                    project_id,
                    std::vector<ConstraintRule>{rule},
                    statistics.data);
                execution.check_ms =
                    std::chrono::duration<double, std::milli>(
                        std::chrono::steady_clock::now() - check_started)
                        .count();
                return execution;
            }));
    }
    bool group_failed = false;
    std::vector<ConstraintGroupExecution> executions;
    executions.reserve(group_futures.size());
    for (auto& future : group_futures) {
        auto execution = future.get();
        constraint_query_ms += execution.query_ms;
        constraint_check_ms += execution.check_ms;
        if (!isSuccessful(execution.result.operation.code)) {
            if (!group_failed) {
                check = execution.result;
            }
            group_failed = true;
            continue;
        }
        check.evaluated_count += execution.result.evaluated_count;
        check.pending_count += execution.result.pending_count;
        check.violations.insert(
            check.violations.end(),
            execution.result.violations.begin(),
            execution.result.violations.end());
        executions.push_back(std::move(execution));
    }
    const auto constraint_group_finished = std::chrono::steady_clock::now();
    if (!group_failed) {
        check.violations.clear();
        {
            std::lock_guard state_lock(constraint_state_mutex_);
            auto& project_states = constraint_states_[project_id];
            for (const auto& execution : executions) {
                for (const auto& rule : execution.rules) {
                    auto& state = project_states[rule.constraint_id];
                    if (state.initialized &&
                        window_update.update_generation <
                            state.update_generation) {
                        continue;
                    }
                    if (execution.range) {
                        auto first = state.violations.lower_bound(
                            execution.range->start_time);
                        auto last = execution.range->end_time ==
                                std::numeric_limits<Timestamp>::max()
                            ? state.violations.end()
                            : state.violations.upper_bound(
                                  execution.range->end_time);
                        state.violations.erase(first, last);
                        if (window_update.window_start_time) {
                            state.violations.erase(
                                state.violations.begin(),
                                state.violations.lower_bound(
                                    *window_update.window_start_time));
                        }
                    } else {
                        state.violations.clear();
                    }
                    for (const auto& violation :
                         execution.result.violations) {
                        if (violation.constraint_id == rule.constraint_id) {
                            state.violations[violation.anchor_time] =
                                violation;
                        }
                    }
                    state.initialized = true;
                    state.update_generation =
                        window_update.update_generation;
                }
            }

            std::unordered_map<std::string, std::vector<const ConstraintRule*>>
                clauses;
            for (const auto& rule : selected_rules) {
                clauses[clauseKey(rule)].push_back(&rule);
            }
            for (const auto& [key, clause] : clauses) {
                (void)key;
                const bool clause_satisfied = std::any_of(
                    clause.begin(), clause.end(),
                    [&project_states](const ConstraintRule* rule) {
                        const auto found = project_states.find(
                            rule->constraint_id);
                        return found == project_states.end() ||
                            !found->second.initialized ||
                            found->second.violations.empty();
                    });
                if (clause_satisfied) {
                    continue;
                }
                for (const auto* rule : clause) {
                    const auto& violations =
                        project_states.at(rule->constraint_id).violations;
                    for (const auto& [anchor, violation] : violations) {
                        (void)anchor;
                        check.violations.push_back(violation);
                    }
                }
            }
        }
        check.satisfied = check.violations.empty();
        std::string message = check.satisfied
            ? "continuous constraint checks completed; all satisfied"
            : "continuous constraint checks completed; violations found";
        if (check.pending_count != 0) {
            message += "; " + std::to_string(check.pending_count) +
                " aligned samples pending";
        }
        check.operation = internal::ok(
            check.evaluated_count,
            std::move(message));
    }
    if (diagnostics) {
        std::lock_guard lock(diagnostics->mutex);
        diagnostics->constraint_group_span_ms =
            std::chrono::duration<double, std::milli>(
                constraint_group_finished - constraint_group_started).count();
        diagnostics->constraint_query_ms = constraint_query_ms;
        diagnostics->constraint_check_ms = constraint_check_ms;
    }
    if (!isSuccessful(check.operation.code)) {
        finishDerived();
        result.constraint_notification_result = check.operation;
        return result;
    }
    if (check.violations.empty()) {
        finishDerived();
        result.constraint_notification_result = internal::ok(
            check.evaluated_count,
            check.pending_count == 0
                ? "no constraint violations; notification skipped"
                : "no currently evaluable constraint violations; " +
                    std::to_string(check.pending_count) +
                    " aligned samples pending");
        return result;
    }

    std::unordered_map<std::string, std::vector<SequenceId>>
        sequences_by_constraint;
    for (const auto& rule : enabled_rules) {
        sequences_by_constraint[rule.constraint_id] = sequenceIdsFor(rule);
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
    const auto constraint_notify_started = std::chrono::steady_clock::now();
    result.constraint_notification_result =
        constraint_notification_executor_.tryEnqueue(
            project_id,
            window_update.affected_end_time.value_or(
                window_update.window_start_time.value_or(0)),
            std::move(violated_constraint_ids),
            std::move(violated_sequence_ids));
    const auto constraint_notify_finished = std::chrono::steady_clock::now();
    if (diagnostics) {
        std::lock_guard lock(diagnostics->mutex);
        diagnostics->constraint_notify_ms =
            std::chrono::duration<double, std::milli>(
                constraint_notify_finished - constraint_notify_started).count();
    }
    finishDerived();
    return result;
}

::grpc::Status TimeseriesCoreGrpcService::ingestData(
    ::grpc::ServerContext* context,
    const pb::IngestDataRequest* request,
    pb::IngestDataResponse* response) {
    (void)context;
    return guardedCall("ingestData", response, [&] {
        const auto project_id = projectFor(request->project_id());
        response->set_project_id(project_id);
        const auto request_started = std::chrono::steady_clock::now();
        const auto diagnostics = shouldCollectIngestDiagnostics()
            ? std::make_shared<IngestDiagnostics>()
            : std::shared_ptr<IngestDiagnostics>{};
        if (diagnostics) {
            diagnostics->request_points = request->points_size();
        }
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
            converted.project_id = project_id;
            input.push_back(std::move(converted));
        }

        const auto resolve_started = std::chrono::steady_clock::now();
        auto resolved = std::make_shared<IngestResult>(
            ingest_service_.ingestAndResolveData(project_id, input));
        const auto resolve_finished = std::chrono::steady_clock::now();
        if (diagnostics) {
            diagnostics->resolved_points = resolved->resolved_data.points.size();
            diagnostics->resolve_ms = std::chrono::duration<double, std::milli>(
                resolve_finished - resolve_started).count();
            diagnostics->submitted = std::chrono::steady_clock::now();
        }
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
                [this, project_id, diagnostics](
                    std::size_t writer_index,
                    const TimeseriesBatch& data) {
                    const auto started = std::chrono::steady_clock::now();
                    const auto result = storage_service_.writeRawDataOnConnection(
                        project_id,
                        writer_index,
                        data);
                    const auto finished = std::chrono::steady_clock::now();
                    if (diagnostics) {
                        const auto elapsed = std::chrono::duration<double, std::milli>(
                            finished - started).count();
                        std::lock_guard lock(diagnostics->mutex);
                        if (!diagnostics->has_first_lane_start ||
                            started < diagnostics->first_lane_start) {
                            diagnostics->first_lane_start = started;
                            diagnostics->has_first_lane_start = true;
                        }
                        if (!diagnostics->has_cold_start ||
                            started < diagnostics->first_cold_start) {
                            diagnostics->first_cold_start = started;
                            diagnostics->has_cold_start = true;
                        }
                        if (!diagnostics->has_cold_end ||
                            finished > diagnostics->last_cold_end) {
                            diagnostics->last_cold_end = finished;
                        }
                        diagnostics->has_cold_end = true;
                        ++diagnostics->cold_shard_count;
                        auto& writer = diagnostics->writers[writer_index];
                        ++writer.shard_count;
                        writer.point_count += data.points.size();
                        writer.write_sum_ms += elapsed;
                        writer.write_max_ms = std::max(
                            writer.write_max_ms, elapsed);
                        std::clog << "cold_write_async"
                                  << " writer=" << writer_index
                                  << " points=" << data.points.size()
                                  << " elapsed_ms=" << elapsed
                                  << " storage_code="
                                  << operationCodeName(result.code);
                        if (!result.message.empty()) {
                            std::clog << " message=\"" << result.message
                                      << "\"";
                        }
                        std::clog << '\n';
                    }
                    if (result.code != OperationCode::Ok &&
                        result.code != OperationCode::PartialSuccess) {
                        std::cerr << "cold_write_async_failed"
                                  << " writer=" << writer_index
                                  << " points=" << data.points.size()
                                  << " storage_code="
                                  << operationCodeName(result.code)
                                  << " message=\"" << result.message
                                  << "\"\n";
                    }
                    return result;
                },
                [this, project_id, diagnostics](const TimeseriesBatch& data) {
                    const auto started = std::chrono::steady_clock::now();
                    const auto result = processHotIngest(
                        project_id, data, diagnostics);
                    const auto finished = std::chrono::steady_clock::now();
                    if (diagnostics) {
                        std::lock_guard lock(diagnostics->mutex);
                        if (!diagnostics->has_first_lane_start ||
                            started < diagnostics->first_lane_start) {
                            diagnostics->first_lane_start = started;
                            diagnostics->has_first_lane_start = true;
                        }
                        diagnostics->hot_start = started;
                        diagnostics->hot_end = finished;
                    }
                    return result;
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
                const auto pipeline = submission.hot_completion.get();
                storage_result = internal::ok(
                    0,
                    "cold storage queued for asynchronous TDengine persistence");
                window_result = pipeline.window_result;
                derived_result = pipeline.derived_result;
                constraint_notification_result =
                    pipeline.constraint_notification_result;
                response->set_storage_queued(true);
                ingest_result = combineIngestResults(
                    resolved->operation,
                    storage_result,
                    window_result,
                    derived_result);
                if (ingest_result.code == OperationCode::Ok) {
                    ingest_result.message =
                        "ingest data accepted; cold storage queued, hot "
                        "window updated and derived windows refreshed";
                } else if (!ingest_result.message.empty()) {
                    ingest_result.message +=
                        "; cold storage queued asynchronously";
                }
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
        if (diagnostics) {
            const auto completed = std::chrono::steady_clock::now();
            std::lock_guard lock(diagnostics->mutex);
            const auto duration = [](auto start, auto end) {
                return std::chrono::duration<double, std::milli>(
                    end - start).count();
            };
            const auto queue_wait_ms = diagnostics->has_first_lane_start
                ? duration(diagnostics->submitted,
                           diagnostics->first_lane_start)
                : 0.0;
            const auto cold_span_ms = diagnostics->has_cold_end
                ? duration(diagnostics->first_cold_start,
                           diagnostics->last_cold_end)
                : 0.0;
            const auto hot_ms = diagnostics->hot_end !=
                    IngestDiagnostics::Clock::time_point{}
                ? duration(diagnostics->hot_start, diagnostics->hot_end)
                : 0.0;
            std::ostringstream line;
            line << "ingest_diag"
                 << " request_points=" << diagnostics->request_points
                 << " resolved_points=" << diagnostics->resolved_points
                 << " cold_shards=" << diagnostics->cold_shard_count
                 << " resolve_ms=" << diagnostics->resolve_ms
                 << " queue_wait_ms=" << queue_wait_ms
                 << " cold_span_ms=" << cold_span_ms
                 << " hot_ms=" << hot_ms
                 << " hot_window_ms=" << diagnostics->hot_window_ms
                 << " hot_after_window_ms="
                 << std::max(0.0, hot_ms - diagnostics->hot_window_ms)
                 << " config_snapshot_ms="
                 << diagnostics->config_snapshot_ms
                 << " constraint_prepare_ms="
                 << diagnostics->constraint_prepare_ms
                 << " window_seq_wait_ms="
                 << diagnostics->window_sequence_lock_wait_ms
                 << " window_seq_work_ms="
                 << diagnostics->window_sequence_update_ms
                 << " window_evict_wait_ms="
                 << diagnostics->window_eviction_lock_wait_ms
                 << " window_evict_work_ms="
                 << diagnostics->window_eviction_update_ms
                 << " derived_ms=" << diagnostics->derived_ms
                 << " constraint_query_ms="
                 << diagnostics->constraint_query_ms
                 << " constraint_check_ms="
                 << diagnostics->constraint_check_ms
                 << " constraint_group_span_ms="
                 << diagnostics->constraint_group_span_ms
                 << " constraint_notify_ms="
                 << diagnostics->constraint_notify_ms
                 << " window_incremental_safe="
                 << (diagnostics->window_incremental_safe ? "true" : "false")
                 << " window_safe_sequences="
                 << diagnostics->window_incremental_safe_sequence_count << "/"
                 << diagnostics->window_sequence_count
                 << " window_seq_tasks="
                 << diagnostics->window_sequence_task_count
                 << " window_seq_groups="
                 << diagnostics->window_sequence_group_count
                 << " window_evicted="
                 << (diagnostics->window_evicted ? "true" : "false")
                 << " pipeline_ms=" << duration(
                        diagnostics->submitted, completed)
                 << " handler_ms=" << duration(request_started, completed)
                 << " storage_code="
                 << operationCodeName(storage_result.code)
                 << " storage_success=" << storage_result.success_count
                 << " storage_failed=" << storage_result.failed_count
                 << " storage_queued="
                 << (response->storage_queued() ? "true" : "false")
                 << " window_code="
                 << operationCodeName(window_result.code)
                 << " derived_code="
                 << operationCodeName(derived_result.code)
                 << " constraint_code="
                 << operationCodeName(constraint_notification_result.code)
                 << " operation_code="
                 << operationCodeName(ingest_result.code);
            if (diagnosticFlagEnabled("SFKG_INGEST_DIAGNOSTIC_WRITERS")) {
                for (const auto& [writer_index, writer] : diagnostics->writers) {
                    line << " writer_" << writer_index
                         << "_shards=" << writer.shard_count
                         << "_points=" << writer.point_count
                         << "_write_sum_ms=" << writer.write_sum_ms
                         << "_write_max_ms=" << writer.write_max_ms;
                }
            }
            // PartialSuccess is accepted by the pipeline, but its message
            // often contains the actual cold-storage or notification error.
            if (ingest_result.code != OperationCode::Ok) {
                line << " operation_message=\"" << ingest_result.message
                     << "\"";
            }
            std::clog << line.str() << '\n';
        }
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
        const auto project_id = projectFor(request->project_id());
        response->set_project_id(project_id);
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
            converted.project_id = project_id;
            input.push_back(std::move(converted));
        }
        const auto result = ingest_service_.ingestAndResolveData(
            project_id, input);
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
        const auto project_id = projectFor(request->project_id());
        response->set_project_id(project_id);
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
        data.project_id = project_id;
        for (auto& point : data.points) {
            point.project_id = project_id;
        }
        conversion::toProto(
            storage_service_.writeRawData(project_id, data),
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
        const auto project_id = projectFor(request->project_id());
        response->set_project_id(project_id);
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
        data.project_id = project_id;
        for (auto& point : data.points) {
            point.project_id = project_id;
        }
        conversion::toProto(
            window_service_.buildTimeWindow(
                project_id, data, request->window_size()),
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
        const auto project_id = projectFor(request->project_id());
        response->set_project_id(project_id);
        WindowQuery query = conversion::fromProto(*request);
        query.project_id = project_id;
        if (query.sequence_ids.empty() ||
            !validTimeRange(query.start_time, query.end_time)) {
            conversion::toProto(
                internal::invalidArgument(
                    "sequence_ids must not be empty and time range must be ordered"),
                response->mutable_operation());
            return ::grpc::Status::OK;
        }
        const auto result = window_service_.queryWindowData(project_id, query);
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
        const auto project_id = projectFor(request->project_id());
        response->set_project_id(project_id);
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
        config.project_id = project_id;

        WindowData data;
        if (request->source_case() == pb::AlignWindowDataRequest::kData) {
            if (!conversion::fromProto(request->data(), &data, &error)) {
                conversion::toProto(
                    internal::invalidArgument(error),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
            data.project_id = project_id;
            for (auto& [sequence_id, points] : data.sequence_values) {
                for (auto& point : points) {
                    point.project_id = project_id;
                }
            }
        } else if (request->source_case() ==
                   pb::AlignWindowDataRequest::kWindowQuery) {
            auto query = conversion::fromProto(request->window_query());
            query.project_id = project_id;
            const auto window_result = window_service_.queryWindowData(
                project_id, query);
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
            const auto relation = config_registry_.findRelation(
                project_id, relation_id);
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
            project_id, data, config, relations);
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
        const auto project_id = projectFor(request->project_id());
        response->set_project_id(project_id);
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
            data.project_id = project_id;
            conversion::toProto(
                statistics_service_.computeBasicStatistics(project_id, data), response);
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
            data.project_id = project_id;

            if (request->relation_id().empty()) {
                conversion::toProto(
                    internal::invalidArgument(
                        "relation_id is required for aligned statistics"),
                    response->mutable_operation());
                return ::grpc::Status::OK;
            }
            const auto relation =
                config_registry_.findRelation(
                    project_id, request->relation_id());
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
                statistics_service_.computeBasicStatistics(
                    project_id, data, *relation),
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
            auto query = conversion::fromProto(request->window_query());
            query.project_id = project_id;
            const auto window_result = window_service_.queryWindowData(
                project_id, query);
            if (!isSuccessful(window_result.operation.code)) {
                conversion::toProto(
                    window_result.operation, response->mutable_operation());
                return ::grpc::Status::OK;
            }
            conversion::toProto(
                statistics_service_.computeBasicStatistics(
                    project_id, window_result.data),
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
        const auto project_id = projectFor(request->project_id());
        response->set_project_id(project_id);
        if (request->constraint_ids().empty()) {
            conversion::toProto(
                internal::invalidArgument("constraint_ids must not be empty"),
                response->mutable_operation());
            return ::grpc::Status::OK;
        }
        const std::vector<std::string> constraint_ids(
            request->constraint_ids().begin(), request->constraint_ids().end());
        const auto lookup = config_registry_.lookupConstraints(
            project_id, constraint_ids);
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
            data.project_id = project_id;
            conversion::toProto(
                constraint_engine_.checkConstraints(project_id, rules, data),
                response);
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
            data.project_id = project_id;
            conversion::toProto(
                constraint_engine_.checkConstraints(project_id, rules, data),
                response);
            return ::grpc::Status::OK;
        }

        if (request->source_case() ==
            pb::CheckConstraintsRequest::kWindowQuery) {
            auto query = conversion::fromProto(request->window_query());
            query.project_id = project_id;
            const auto window_result = window_service_.queryWindowData(
                project_id, query);
            if (!isSuccessful(window_result.operation.code)) {
                conversion::toProto(
                    window_result.operation, response->mutable_operation());
                return ::grpc::Status::OK;
            }
            conversion::toProto(
                constraint_engine_.checkConstraints(
                    project_id, rules, window_result.data),
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
        const auto project_id = projectFor(request->project_id());
        response->set_project_id(project_id);
        HistoryQuery query = conversion::fromProto(*request);
        query.project_id = project_id;
        if (query.sequence_ids.empty() || query.start_time > query.end_time ||
            (query.granularity && *query.granularity <= 0)) {
            conversion::toProto(
                internal::invalidArgument(
                    "history query sequence_ids, range or granularity is invalid"),
                response->mutable_operation());
            return ::grpc::Status::OK;
        }
        const auto result = history_service_.queryHistoryData(project_id, query);
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
        const auto project_id = projectFor(request->project_id());
        response->set_project_id(project_id);
        HistoryOverviewQuery query =
            conversion::fromProto(*request);
        query.project_id = project_id;
        if (query.start_time && query.end_time &&
            *query.start_time > *query.end_time) {
            conversion::toProto(
                internal::invalidArgument(
                    "history overview time range is invalid"),
                response->mutable_operation());
            return ::grpc::Status::OK;
        }
        const auto result = history_service_.queryHistoryOverview(
            project_id, query);
        conversion::toProto(result, response);
        return ::grpc::Status::OK;
    });
}

}  // namespace sfkg::timeseries::core::grpc
