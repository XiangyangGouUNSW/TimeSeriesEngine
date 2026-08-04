#include "grpc/internal/proto_conversion.hpp"

#include <cmath>
#include <type_traits>
#include <utility>

namespace sfkg::timeseries::core::grpc::conversion {
namespace {

pb::OperationCode operationCodeToProto(OperationCode code) {
    switch (code) {
        case OperationCode::Ok:
            return pb::OPERATION_CODE_OK;
        case OperationCode::PartialSuccess:
            return pb::OPERATION_CODE_PARTIAL_SUCCESS;
        case OperationCode::InvalidArgument:
            return pb::OPERATION_CODE_INVALID_ARGUMENT;
        case OperationCode::NotFound:
            return pb::OPERATION_CODE_NOT_FOUND;
        case OperationCode::FailedPrecondition:
            return pb::OPERATION_CODE_FAILED_PRECONDITION;
        case OperationCode::Unavailable:
            return pb::OPERATION_CODE_UNAVAILABLE;
        case OperationCode::InternalError:
            return pb::OPERATION_CODE_INTERNAL_ERROR;
        case OperationCode::NotImplemented:
            return pb::OPERATION_CODE_NOT_IMPLEMENTED;
    }
    return pb::OPERATION_CODE_INTERNAL_ERROR;
}

bool fromProto(
    const pb::RawTimeseriesPoint& source,
    RawTimeseriesPoint* target,
    std::string* error) {
    if (source.sequence_id().empty()) {
        *error = "sequence_id must not be empty";
        return false;
    }
    if (!source.has_value() ||
        !conversion::fromProto(source.value(), &target->value, error)) {
        return false;
    }
    target->time = source.time();
    target->sequence_id = source.sequence_id();
    return true;
}

void toProto(
    const RawTimeseriesPoint& source,
    pb::RawTimeseriesPoint* target) {
    target->set_time(source.time);
    target->set_sequence_id(source.sequence_id);
    conversion::toProto(source.value, target->mutable_value());
}

bool fromProto(
    const pb::ConstraintRule& source,
    ConstraintRule* target,
    std::string* error) {
    if (source.constraint_id().empty()) {
        *error = "constraint_id must not be empty";
        return false;
    }
    target->constraint_id = source.constraint_id();
    target->lower_bound = source.lower_bound();
    target->upper_bound = source.upper_bound();
    target->variable_mapping.clear();
    target->terms.clear();
    for (const auto& [variable, sequence_id] : source.variable_mapping()) {
        target->variable_mapping.emplace(variable, sequence_id);
    }
    target->terms.reserve(source.terms_size());
    for (const auto& term : source.terms()) {
        target->terms.push_back(
            {term.variable(), term.coefficient(),
             static_cast<std::size_t>(term.sample_offset())});
    }
    return true;
}

}  // namespace

void toProto(const OperationResult& source, pb::OperationResult* target) {
    target->set_code(operationCodeToProto(source.code));
    target->set_success_count(source.success_count);
    target->set_failed_count(source.failed_count);
    target->set_message(source.message);
}

bool fromProto(
    const pb::TimeseriesValue& source,
    TimeseriesValue* target,
    std::string* error) {
    switch (source.kind_case()) {
        case pb::TimeseriesValue::kDoubleValue:
            *target = source.double_value();
            return true;
        case pb::TimeseriesValue::kInt64Value:
            *target = source.int64_value();
            return true;
        case pb::TimeseriesValue::kBoolValue:
            *target = source.bool_value();
            return true;
        case pb::TimeseriesValue::kStringValue:
            *target = source.string_value();
            return true;
        case pb::TimeseriesValue::KIND_NOT_SET:
            *error = "timeseries value kind is not set";
            return false;
    }
    *error = "unknown timeseries value kind";
    return false;
}

void toProto(const TimeseriesValue& source, pb::TimeseriesValue* target) {
    std::visit(
        [target](const auto& value) {
            using Value = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<Value, double>) {
                target->set_double_value(value);
            } else if constexpr (std::is_same_v<Value, std::int64_t>) {
                target->set_int64_value(value);
            } else if constexpr (std::is_same_v<Value, bool>) {
                target->set_bool_value(value);
            } else {
                target->set_string_value(value);
            }
        },
        source);
}

bool fromProto(
    const pb::TimeseriesIngestData& source,
    TimeseriesIngestData* target,
    std::string* error) {
    if (source.has_sequence_id()) {
        target->sequence_id = source.sequence_id();
    } else {
        target->sequence_id.reset();
    }
    if (!target->sequence_id.has_value() &&
        (source.data_source_id().empty() ||
         source.external_sequence_id().empty())) {
        *error = "sequence_id or both external identifiers are required";
        return false;
    }
    if (!source.has_value() ||
        !fromProto(source.value(), &target->value, error)) {
        return false;
    }
    target->data_source_id = source.data_source_id();
    target->external_sequence_id = source.external_sequence_id();
    target->time = source.time();
    return true;
}

bool fromProto(
    const pb::TimeseriesBatch& source,
    TimeseriesBatch* target,
    std::string* error) {
    target->points.clear();
    target->points.reserve(source.points_size());
    for (const auto& point : source.points()) {
        RawTimeseriesPoint converted;
        if (!fromProto(point, &converted, error)) {
            return false;
        }
        target->points.push_back(std::move(converted));
    }
    return true;
}

void toProto(const TimeseriesBatch& source, pb::TimeseriesBatch* target) {
    target->clear_points();
    for (const auto& point : source.points) {
        toProto(point, target->add_points());
    }
}

bool fromProto(
    const pb::WindowData& source,
    WindowData* target,
    std::string* error) {
    target->window_start_time = source.window_start_time();
    target->window_end_time = source.window_end_time();
    target->sequence_values.clear();
    for (const auto& sequence : source.sequences()) {
        if (sequence.sequence_id().empty()) {
            *error = "window sequence_id must not be empty";
            return false;
        }
        auto& points = target->sequence_values[sequence.sequence_id()];
        points.reserve(sequence.points_size());
        for (const auto& point : sequence.points()) {
            if (!point.has_value()) {
                *error = "window point value is not set";
                return false;
            }
            RawTimeseriesPoint converted;
            converted.time = point.time();
            converted.sequence_id = sequence.sequence_id();
            if (!fromProto(point.value(), &converted.value, error)) {
                return false;
            }
            points.push_back(std::move(converted));
        }
    }
    return true;
}

void toProto(const WindowData& source, pb::WindowData* target) {
    target->set_window_start_time(source.window_start_time);
    target->set_window_end_time(source.window_end_time);
    target->clear_sequences();
    for (const auto& [sequence_id, points] : source.sequence_values) {
        auto* sequence = target->add_sequences();
        sequence->set_sequence_id(sequence_id);
        for (const auto& point : points) {
            auto* converted = sequence->add_points();
            converted->set_time(point.time);
            toProto(point.value, converted->mutable_value());
        }
    }
}

bool fromProto(
    const pb::AlignedWindowData& source,
    AlignedWindowData* target,
    std::string* error) {
    target->window_start_time = source.window_start_time();
    target->window_end_time = source.window_end_time();
    target->samples.clear();
    target->samples.reserve(source.samples_size());
    for (const auto& sample : source.samples()) {
        AlignedSample converted;
        converted.time = sample.time();
        for (const auto& value : sample.values()) {
            if (value.sequence_id().empty() || !value.has_value()) {
                *error = "aligned value must contain sequence_id and value";
                return false;
            }
            TimeseriesValue converted_value;
            if (!fromProto(value.value(), &converted_value, error)) {
                return false;
            }
            if (!converted.values.emplace(
                    value.sequence_id(), std::move(converted_value)).second) {
                *error = "aligned sample contains a duplicate sequence_id";
                return false;
            }
        }
        target->samples.push_back(std::move(converted));
    }
    return true;
}

void toProto(
    const AlignedWindowData& source,
    pb::AlignedWindowData* target) {
    target->set_window_start_time(source.window_start_time);
    target->set_window_end_time(source.window_end_time);
    target->clear_samples();
    for (const auto& sample : source.samples) {
        auto* converted_sample = target->add_samples();
        converted_sample->set_time(sample.time);
        for (const auto& [sequence_id, value] : sample.values) {
            auto* converted_value = converted_sample->add_values();
            converted_value->set_sequence_id(sequence_id);
            toProto(value, converted_value->mutable_value());
        }
    }
}

bool fromProto(
    const pb::AlignmentConfig& source,
    AlignmentConfig* target,
    std::string* error) {
    if (source.bucket_interval() <= 0) {
        *error = "bucket_interval must be positive";
        return false;
    }
    target->bucket_interval = source.bucket_interval();
    target->sequences.clear();
    target->sequences.reserve(source.sequences_size());
    for (const auto& sequence : source.sequences()) {
        SequenceAlignmentConfig converted;
        converted.sequence_id = sequence.sequence_id();
        if (converted.sequence_id.empty()) {
            *error = "alignment sequence_id must not be empty";
            return false;
        }
        switch (sequence.role()) {
            case pb::VARIABLE_ROLE_INDEPENDENT:
                converted.role = VariableRole::Independent;
                break;
            case pb::VARIABLE_ROLE_DEPENDENT:
                converted.role = VariableRole::Dependent;
                break;
            default:
                *error = "alignment variable role is unspecified";
                return false;
        }
        switch (sequence.aggregation()) {
            case pb::BUCKET_AGGREGATION_FIRST:
                converted.aggregation = BucketAggregation::First;
                break;
            case pb::BUCKET_AGGREGATION_LAST:
                converted.aggregation = BucketAggregation::Last;
                break;
            case pb::BUCKET_AGGREGATION_AVERAGE:
                converted.aggregation = BucketAggregation::Average;
                break;
            case pb::BUCKET_AGGREGATION_MAXIMUM:
                converted.aggregation = BucketAggregation::Maximum;
                break;
            case pb::BUCKET_AGGREGATION_MINIMUM:
                converted.aggregation = BucketAggregation::Minimum;
                break;
            default:
                *error = "alignment aggregation is unspecified";
                return false;
        }
        switch (sequence.fill_method()) {
            case pb::GAP_FILL_METHOD_NEAR:
                converted.fill_method = GapFillMethod::Near;
                break;
            case pb::GAP_FILL_METHOD_PREVIOUS:
                converted.fill_method = GapFillMethod::Previous;
                break;
            case pb::GAP_FILL_METHOD_NEXT:
                converted.fill_method = GapFillMethod::Next;
                break;
            case pb::GAP_FILL_METHOD_LINEAR:
                converted.fill_method = GapFillMethod::Linear;
                break;
            default:
                *error = "alignment fill method is unspecified";
                return false;
        }
        target->sequences.push_back(std::move(converted));
    }
    return true;
}

bool fromProto(
    const pb::RuntimeInstanceConfig& source,
    RuntimeInstanceConfig* target,
    std::string* error) {
    (void)error;
    target->sequence_id = source.sequence_id();
    target->data_source_id = source.data_source_id();
    target->external_sequence_id = source.external_sequence_id();
    target->category_id = source.category_id();
    target->data_type = source.data_type();
    return true;
}

bool fromProto(
    const pb::RuntimeConstraintConfig& source,
    RuntimeConstraintConfig* target,
    std::string* error) {
    if (!source.has_rule()) {
        *error = "runtime constraint rule is not set";
        return false;
    }
    target->enabled = source.enabled();
    return fromProto(source.rule(), &target->rule, error);
}

bool fromProto(
    const pb::RelationLagRange& source,
    RelationLagRange* target,
    std::string* error) {
    (void)error;
    target->min = source.min();
    target->max = source.max();
    return true;
}

bool fromProto(
    const pb::RuntimeRelationSource& source,
    RuntimeRelationSource* target,
    std::string* error) {
    if (source.source_category_id().empty()) {
        *error = "relation source_category_id must not be empty";
        return false;
    }
    if (!std::isfinite(source.weight())) {
        *error = "relation source weight must be finite";
        return false;
    }

    target->source_category_id = source.source_category_id();
    target->weight = source.weight();
    switch (source.lag_spec_case()) {
        case pb::RuntimeRelationSource::kFixedLag:
            target->lag = source.fixed_lag();
            return true;
        case pb::RuntimeRelationSource::kLagRange:
            target->lag = RelationLagRange{
                source.lag_range().min(), source.lag_range().max()};
            return true;
        case pb::RuntimeRelationSource::LAG_SPEC_NOT_SET:
            target->lag = std::monostate{};
            return true;
    }
    *error = "unknown relation lag specification";
    return false;
}

bool fromProto(
    const pb::RuntimeRelationConfig& source,
    RuntimeRelationConfig* target,
    std::string* error) {
    target->relation_id = source.relation_id();
    target->sources.clear();
    target->sources.reserve(source.sources_size());
    for (const auto& item : source.sources()) {
        RuntimeRelationSource converted;
        if (!fromProto(item, &converted, error)) {
            return false;
        }
        target->sources.push_back(std::move(converted));
    }
    target->target_category_id = source.target_category_id();
    target->relation_type = source.relation_type();
    target->confidence = source.confidence();
    target->enabled = source.enabled();
    return true;
}

WindowQuery fromProto(const pb::QueryWindowDataRequest& source) {
    WindowQuery result;
    result.sequence_ids.assign(
        source.sequence_ids().begin(), source.sequence_ids().end());
    if (source.has_start_time()) {
        result.start_time = source.start_time();
    }
    if (source.has_end_time()) {
        result.end_time = source.end_time();
    }
    return result;
}

HistoryQuery fromProto(const pb::QueryHistoryDataRequest& source) {
    HistoryQuery result;
    result.sequence_ids.assign(
        source.sequence_ids().begin(), source.sequence_ids().end());
    result.start_time = source.start_time();
    result.end_time = source.end_time();
    if (source.has_granularity()) {
        result.granularity = source.granularity();
    }
    return result;
}

HistoryOverviewQuery fromProto(
    const pb::QueryHistoryOverviewRequest& source) {
    HistoryOverviewQuery result;
    result.sequence_ids.assign(
        source.sequence_ids().begin(), source.sequence_ids().end());
    if (source.has_start_time()) {
        result.start_time = source.start_time();
    }
    if (source.has_end_time()) {
        result.end_time = source.end_time();
    }
    return result;
}

void toProto(
    const StatisticsResult& source,
    pb::ComputeStatisticsResponse* target) {
    toProto(source.operation, target->mutable_operation());
    target->clear_sequence_metrics();
    for (const auto& [sequence_id, metrics] : source.sequence_metrics) {
        auto* sequence = target->add_sequence_metrics();
        sequence->set_sequence_id(sequence_id);
        for (const auto& [name, value] : metrics) {
            auto* metric = sequence->add_metrics();
            metric->set_name(name);
            toProto(value, metric->mutable_value());
        }
    }
    if (source.correlation_vector) {
        auto* correlation = target->mutable_correlation_vector();
        correlation->set_dependent_sequence_id(
            source.correlation_vector->dependent_sequence_id);
        for (const auto& item : source.correlation_vector->correlations) {
            auto* converted = correlation->add_correlations();
            converted->set_independent_sequence_id(
                item.independent_sequence_id);
            converted->set_coefficient(item.coefficient);
        }
    }
}

void toProto(
    const ConstraintCheckResult& source,
    pb::CheckConstraintsResponse* target) {
    toProto(source.operation, target->mutable_operation());
    target->set_satisfied(source.satisfied);
    target->set_evaluated_count(source.evaluated_count);
    target->clear_violations();
    for (const auto& violation : source.violations) {
        auto* converted = target->add_violations();
        converted->set_constraint_id(violation.constraint_id);
        converted->set_anchor_time(violation.anchor_time);
        converted->set_lower_bound(violation.lower_bound);
        converted->set_upper_bound(violation.upper_bound);
        converted->set_evaluated_value(violation.evaluated_value);
        for (const auto& term : violation.term_values) {
            auto* converted_term = converted->add_term_values();
            converted_term->set_variable(term.variable);
            converted_term->set_sequence_id(term.sequence_id);
            converted_term->set_coefficient(term.coefficient);
            converted_term->set_sample_offset(term.sample_offset);
            converted_term->set_sample_time(term.sample_time);
            converted_term->set_value(term.value);
        }
    }
}

void toProto(
    const HistoryOverviewResult& source,
    pb::QueryHistoryOverviewResponse* target) {
    toProto(source.operation, target->mutable_operation());
    auto* overview = target->mutable_overview();
    overview->set_total_point_count(source.overview.total_point_count);
    overview->set_sequence_count(source.overview.sequence_count);
    overview->clear_column_names();
    for (const auto& column_name : source.overview.column_names) {
        overview->add_column_names(column_name);
    }
    overview->clear_series();
    for (const auto& series : source.overview.series) {
        auto* converted = overview->add_series();
        converted->set_sequence_id(series.sequence_id);
        converted->set_point_count(series.point_count);
        if (series.first_time) {
            converted->set_first_time(*series.first_time);
        }
        if (series.last_time) {
            converted->set_last_time(*series.last_time);
        }
    }
    if (source.overview.first_time) {
        overview->set_first_time(*source.overview.first_time);
    }
    if (source.overview.last_time) {
        overview->set_last_time(*source.overview.last_time);
    }
}

}  // namespace sfkg::timeseries::core::grpc::conversion
