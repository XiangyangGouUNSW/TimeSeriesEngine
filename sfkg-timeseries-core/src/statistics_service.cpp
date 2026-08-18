#include "sfkg/timeseries/core/statistics_service.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core {
namespace {

bool numericValue(const TimeseriesValue& value, double* output) {
    if (const auto* number = std::get_if<double>(&value)) {
        if (!std::isfinite(*number)) {
            return false;
        }
        *output = *number;
        return true;
    }
    if (const auto* number = std::get_if<std::int64_t>(&value)) {
        *output = static_cast<double>(*number);
        return true;
    }
    return false;
}

OperationResult failedPrecondition(std::string message) {
    return internal::makeOperationResult(
        OperationCode::FailedPrecondition, 0, 0, std::move(message));
}

void addMetric(
    std::unordered_map<std::string, TimeseriesValue>* metrics,
    std::string name,
    TimeseriesValue value) {
    metrics->emplace(std::move(name), std::move(value));
}

void addWindowMetadata(
    const std::vector<RawTimeseriesPoint>& points,
    std::unordered_map<std::string, TimeseriesValue>* metrics) {
    const auto [first, last] = std::minmax_element(
        points.begin(), points.end(),
        [](const auto& left, const auto& right) {
            return left.time < right.time;
        });
    addMetric(
        metrics,
        "count",
        static_cast<std::int64_t>(points.size()));
    addMetric(metrics, "first_time", first->time);
    addMetric(metrics, "last_time", last->time);
}

bool appendCorrelationPair(
    const AlignedSample& sample,
    const SequenceId& target_sequence_id,
    const SequenceId& source_sequence_id,
    std::vector<double>* target_values,
    std::vector<double>* source_values) {
    const auto target = sample.values.find(target_sequence_id);
    const auto source = sample.values.find(source_sequence_id);
    if (target == sample.values.end() || source == sample.values.end()) {
        return false;
    }

    double target_value = 0.0;
    double source_value = 0.0;
    if (!numericValue(target->second, &target_value) ||
        !numericValue(source->second, &source_value)) {
        return false;
    }
    target_values->push_back(target_value);
    source_values->push_back(source_value);
    return true;
}

bool pearsonCorrelation(
    const std::vector<double>& target_values,
    const std::vector<double>& source_values,
    double* coefficient) {
    if (target_values.size() < 2 ||
        target_values.size() != source_values.size()) {
        return false;
    }

    double target_mean = 0.0;
    double source_mean = 0.0;
    for (std::size_t index = 0; index < target_values.size(); ++index) {
        target_mean += target_values[index];
        source_mean += source_values[index];
    }
    target_mean /= static_cast<double>(target_values.size());
    source_mean /= static_cast<double>(source_values.size());

    double covariance = 0.0;
    double target_squared = 0.0;
    double source_squared = 0.0;
    for (std::size_t index = 0; index < target_values.size(); ++index) {
        const auto target_delta = target_values[index] - target_mean;
        const auto source_delta = source_values[index] - source_mean;
        covariance += target_delta * source_delta;
        target_squared += target_delta * target_delta;
        source_squared += source_delta * source_delta;
    }
    if (target_squared <= 0.0 || source_squared <= 0.0) {
        return false;
    }

    *coefficient = covariance / std::sqrt(target_squared * source_squared);
    return std::isfinite(*coefficient);
}

}  // namespace

StatisticsResult StatisticsService::computeBasicStatistics(
    const ProjectId& project_id,
    const WindowData& data) const {
    StatisticsResult result;
    result.project_id = project_id;
    if (data.window_start_time > data.window_end_time) {
        result.operation = internal::invalidArgument(
            "statistics window start time must not be after end time");
        return result;
    }
    if (data.sequence_values.empty()) {
        result.operation = internal::invalidArgument(
            "statistics window must contain at least one sequence");
        return result;
    }

    std::size_t successful_count = 0;
    std::size_t failed_count = 0;
    for (const auto& [sequence_id, points] : data.sequence_values) {
        if (points.empty()) {
            ++failed_count;
            continue;
        }

        std::unordered_map<std::string, TimeseriesValue> metrics;
        addWindowMetadata(points, &metrics);

        std::vector<double> values;
        values.reserve(points.size());
        bool numeric = true;
        for (const auto& point : points) {
            double value = 0.0;
            if (!numericValue(point.value, &value)) {
                numeric = false;
                break;
            }
            values.push_back(value);
        }

        if (!numeric) {
            result.sequence_metrics.emplace(sequence_id, std::move(metrics));
            ++failed_count;
            continue;
        }

        ++successful_count;

        double sum = 0.0;
        double minimum = values.front();
        double maximum = values.front();
        for (const auto value : values) {
            sum += value;
            minimum = std::min(minimum, value);
            maximum = std::max(maximum, value);
        }
        const auto mean = sum / static_cast<double>(values.size());
        double squared_deviation = 0.0;
        for (const auto value : values) {
            const auto deviation = value - mean;
            squared_deviation += deviation * deviation;
        }
        const auto variance = std::max(
            0.0,
            squared_deviation / static_cast<double>(values.size()));

        addMetric(&metrics, "sum", sum);
        addMetric(&metrics, "mean", mean);
        addMetric(&metrics, "min", minimum);
        addMetric(&metrics, "max", maximum);
        addMetric(&metrics, "variance", variance);
        addMetric(&metrics, "stddev", std::sqrt(variance));
        result.sequence_metrics.emplace(sequence_id, std::move(metrics));
    }

    if (successful_count == 0) {
        result.operation = internal::makeOperationResult(
            OperationCode::InvalidArgument,
            0,
            failed_count,
            "no sequence contained usable statistics data");
    } else if (failed_count != 0) {
        result.operation = internal::makeOperationResult(
            OperationCode::PartialSuccess,
            successful_count,
            failed_count,
            "statistics computed with partial sequence results");
    } else {
        result.operation = internal::ok(
            successful_count, "statistics computed");
    }
    return result;
}

StatisticsResult StatisticsService::computeBasicStatistics(
    const WindowData& data) const {
    return computeBasicStatistics(
        data.project_id.empty() ? ProjectId{"default"} : data.project_id,
        data);
}

StatisticsResult StatisticsService::computeBasicStatistics(
    const ProjectId& project_id,
    const AlignedWindowData& data,
    const RuntimeRelationConfig& relation) const {
    StatisticsResult result;
    result.project_id = project_id;
    if (data.window_start_time > data.window_end_time) {
        result.operation = internal::invalidArgument(
            "statistics window start time must not be after end time");
        return result;
    }
    if (data.samples.empty()) {
        result.operation = internal::invalidArgument(
            "aligned statistics window must contain at least one sample");
        return result;
    }
    if (!relation.enabled) {
        result.operation = failedPrecondition(
            "statistics relation must be enabled");
        return result;
    }
    if (relation.target_sequence_id.empty() || relation.sources.empty()) {
        result.operation = internal::invalidArgument(
            "statistics relation must contain a target and sources");
        return result;
    }
    if (!relation.relation_type.empty() &&
        relation.relation_type != "correlation") {
        result.operation = failedPrecondition(
            "statistics relation type must be correlation");
        return result;
    }

    std::unordered_set<SequenceId> source_ids;
    for (const auto& source : relation.sources) {
        if (source.source_sequence_id.empty() ||
            source.source_sequence_id == relation.target_sequence_id) {
            result.operation = internal::invalidArgument(
                "statistics relation source sequence is invalid");
            return result;
        }
        if (!source_ids.emplace(source.source_sequence_id).second) {
            result.operation = internal::invalidArgument(
                "statistics relation contains duplicate source sequences");
            return result;
        }
    }

    WindowData metric_window;
    metric_window.project_id = project_id;
    metric_window.window_start_time = data.window_start_time;
    metric_window.window_end_time = data.window_end_time;
    for (const auto& sample : data.samples) {
        for (const auto& [sequence_id, value] : sample.values) {
            metric_window.sequence_values[sequence_id].push_back({
                sample.time,
                sequence_id,
                value,
                project_id});
        }
    }
    result = computeBasicStatistics(project_id, metric_window);
    const auto metric_failed_count = result.operation.failed_count;

    CorrelationVector correlation;
    correlation.dependent_sequence_id = relation.target_sequence_id;
    std::size_t failed_count = 0;
    for (const auto& source : relation.sources) {
        std::vector<double> target_values;
        std::vector<double> source_values;
        target_values.reserve(data.samples.size());
        source_values.reserve(data.samples.size());
        for (const auto& sample : data.samples) {
            appendCorrelationPair(
                sample,
                relation.target_sequence_id,
                source.source_sequence_id,
                &target_values,
                &source_values);
        }

        double coefficient = 0.0;
        if (!pearsonCorrelation(target_values, source_values, &coefficient)) {
            ++failed_count;
            continue;
        }
        correlation.correlations.push_back({
            source.source_sequence_id,
            coefficient});
    }

    result.correlation_vector = std::move(correlation);
    const auto success_count = result.correlation_vector->correlations.size();
    if (success_count == 0) {
        result.operation = internal::makeOperationResult(
            OperationCode::InvalidArgument,
            0,
            failed_count,
            "no source sequence contained enough varying numeric pairs");
    } else if (failed_count != 0 || metric_failed_count != 0) {
        result.operation = internal::makeOperationResult(
            OperationCode::PartialSuccess,
            success_count,
            failed_count + metric_failed_count,
            "aligned statistics and correlations computed with partial results");
    } else {
        result.operation = internal::ok(
            success_count, "aligned statistics and correlations computed");
    }
    return result;
}

StatisticsResult StatisticsService::computeBasicStatistics(
    const AlignedWindowData& data,
    const RuntimeRelationConfig& relation) const {
    return computeBasicStatistics(
        data.project_id.empty() ? ProjectId{"default"} : data.project_id,
        data,
        relation);
}

}  // namespace sfkg::timeseries::core
