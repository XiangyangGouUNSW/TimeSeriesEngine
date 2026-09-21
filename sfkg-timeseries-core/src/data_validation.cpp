#include "sfkg/timeseries/core/data_validation.hpp"

#include <cmath>
#include <string>

namespace sfkg::timeseries::core::validation {

TimeseriesValueKind expectedValueKind(std::string_view data_type) noexcept {
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

TimeseriesValueKind valueKind(const TimeseriesValue& value) noexcept {
    switch (value.index()) {
        case 0:
            return TimeseriesValueKind::Double;
        case 1:
            return TimeseriesValueKind::Int64;
        case 2:
            return TimeseriesValueKind::Bool;
        case 3:
            return TimeseriesValueKind::String;
        default:
            return TimeseriesValueKind::Unknown;
    }
}

const char* validateValue(
    const TimeseriesValue& value,
    TimeseriesValueKind expected) noexcept {
    if (expected != TimeseriesValueKind::Unknown &&
        valueKind(value) != expected) {
        return "value type does not match registered data_type";
    }
    if (const auto* number = std::get_if<double>(&value);
        number != nullptr && !std::isfinite(*number)) {
        return "double value must be finite";
    }
    if (const auto* text = std::get_if<std::string>(&value);
        text != nullptr && text->size() > kMaxStringValueBytes) {
        return "string value exceeds storage capacity";
    }
    return nullptr;
}

const char* validateRawPoint(const RawTimeseriesPoint& point) noexcept {
    if (point.sequence_id.empty()) {
        return "sequence_id must not be empty";
    }
    if (point.sequence_id.size() > kMaxSequenceIdBytes) {
        return "sequence_id exceeds storage capacity";
    }
    return validateValue(point.value);
}

const char* validateBatchContext(
    const ProjectId& project_id,
    const TimeseriesBatch& batch) noexcept {
    if (project_id.empty()) {
        return "project_id must not be empty";
    }
    if (!batch.project_id.empty() && batch.project_id != project_id) {
        return "batch project_id does not match request project_id";
    }
    return nullptr;
}

const char* validateRawPoint(
    const ProjectId& project_id,
    const RawTimeseriesPoint& point) noexcept {
    if (!point.project_id.empty() && point.project_id != project_id) {
        return "point project_id does not match request project_id";
    }
    return validateRawPoint(point);
}

const char* validateWindowData(
    const ProjectId& project_id,
    const WindowData& data) noexcept {
    if (data.project_id != project_id && !data.project_id.empty()) {
        return "window project_id does not match request project_id";
    }
    if (data.window_start_time > data.window_end_time) {
        return "window start time must not be after end time";
    }

    for (const auto& [sequence_id, points] : data.sequence_values) {
        if (sequence_id.empty()) {
            return "window sequence_id must not be empty";
        }
        if (sequence_id.size() > kMaxSequenceIdBytes) {
            return "sequence_id exceeds storage capacity";
        }
        for (const auto& point : points) {
            if (point.sequence_id != sequence_id) {
                return "window point sequence_id does not match its map key";
            }
            if (const auto* error = validateRawPoint(project_id, point)) {
                return error;
            }
        }
    }
    return nullptr;
}

const char* validateAlignedWindowData(
    const ProjectId& project_id,
    const AlignedWindowData& data) noexcept {
    if (data.project_id != project_id && !data.project_id.empty()) {
        return "aligned window project_id does not match request project_id";
    }
    if (data.window_start_time > data.window_end_time) {
        return "aligned window start time must not be after end time";
    }

    for (std::size_t index = 1; index < data.samples.size(); ++index) {
        if (data.samples[index - 1].time >= data.samples[index].time) {
            return "aligned sample times must be strictly increasing";
        }
    }
    for (const auto& sample : data.samples) {
        for (const auto& [sequence_id, value] : sample.values) {
            if (sequence_id.empty()) {
                return "aligned value sequence_id must not be empty";
            }
            if (sequence_id.size() > kMaxSequenceIdBytes) {
                return "sequence_id exceeds storage capacity";
            }
            if (const auto* error = validateValue(value)) {
                return error;
            }
        }
    }
    return nullptr;
}

}  // namespace sfkg::timeseries::core::validation
