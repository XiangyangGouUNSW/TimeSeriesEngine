#include "sfkg/timeseries/core/ingest_service.hpp"

#include <cmath>
#include <string>
#include <utility>

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core {
namespace {

bool valueMatchesDataType(
    const std::string& data_type,
    const TimeseriesValue& value) {
    if (data_type.empty()) {
        return true;
    }
    if (data_type == "double" || data_type == "float" ||
        data_type == "continuous") {
        return std::holds_alternative<double>(value);
    }
    if (data_type == "int" || data_type == "int64" ||
        data_type == "integer" || data_type == "discrete") {
        return std::holds_alternative<std::int64_t>(value);
    }
    if (data_type == "bool" || data_type == "boolean") {
        return std::holds_alternative<bool>(value);
    }
    if (data_type == "string" || data_type == "text" ||
        data_type == "label" || data_type == "categorical") {
        return std::holds_alternative<std::string>(value);
    }
    // Unknown data_type values are metadata owned by the unified service;
    // preserve compatibility and let the concrete value type decide storage.
    return true;
}

}  // namespace

IngestResult IngestService::ingestAndResolveData(
    const std::vector<TimeseriesIngestData>& input) const {
    IngestResult result;
    if (input.empty()) {
        result.operation = internal::invalidArgument(
            "ingest points must not be empty");
        return result;
    }

    result.resolved_data.points.reserve(input.size());
    OperationCode first_error_code = OperationCode::InvalidArgument;
    std::string first_error;
    std::size_t failed_count = 0;

    for (const auto& point : input) {
        std::optional<SequenceId> resolved_sequence_id;
        std::optional<RuntimeInstanceConfig> instance_config;
        std::string error;
        OperationCode error_code = OperationCode::InvalidArgument;

        if (point.sequence_id) {
            if (point.sequence_id->empty()) {
                error = "sequence_id must not be empty";
            } else if (!(instance_config = configs_.findInstance(
                             *point.sequence_id))) {
                error = "sequence is not registered: " + *point.sequence_id;
                error_code = OperationCode::NotFound;
            } else {
                resolved_sequence_id = *point.sequence_id;
                if (!point.data_source_id.empty() &&
                    !point.external_sequence_id.empty()) {
                    const auto mapped = configs_.resolveSequenceId(
                        point.data_source_id, point.external_sequence_id);
                    if (!mapped || *mapped != *point.sequence_id) {
                        error =
                            "external identifiers do not match sequence_id: " +
                            *point.sequence_id;
                    }
                }
            }
        } else {
            if (point.data_source_id.empty() ||
                point.external_sequence_id.empty()) {
                error =
                    "sequence_id or both external identifiers are required";
            } else {
                resolved_sequence_id = configs_.resolveSequenceId(
                    point.data_source_id, point.external_sequence_id);
                if (!resolved_sequence_id) {
                    error = "sequence is not registered for external identifiers: " +
                        point.data_source_id + ":" + point.external_sequence_id;
                    error_code = OperationCode::NotFound;
                } else {
                    instance_config = configs_.findInstance(
                        *resolved_sequence_id);
                }
            }
        }

        if (error.empty()) {
            if (!instance_config || !valueMatchesDataType(
                    instance_config->data_type, point.value)) {
                error = "value type does not match registered data_type";
            }
        }
        if (error.empty()) {
            if (const auto* value = std::get_if<double>(&point.value);
                value != nullptr && !std::isfinite(*value)) {
                error = "double value must be finite";
            }
        }

        if (!error.empty()) {
            ++failed_count;
            if (first_error.empty()) {
                first_error_code = error_code;
                first_error = std::move(error);
            }
            continue;
        }

        result.resolved_data.points.push_back({
            point.time, *resolved_sequence_id, point.value});
    }

    const auto success_count = result.resolved_data.points.size();
    if (success_count == 0) {
        result.operation = internal::makeOperationResult(
            first_error_code,
            0,
            failed_count,
            first_error.empty() ? "no ingest points were resolved" : first_error);
    } else if (failed_count != 0) {
        result.operation = internal::makeOperationResult(
            OperationCode::PartialSuccess,
            success_count,
            failed_count,
            "some ingest points could not be resolved: " + first_error);
    } else {
        result.operation = internal::ok(
            success_count, "ingest points resolved");
    }
    return result;
}

}  // namespace sfkg::timeseries::core
