#include "sfkg/timeseries/core/derived_series_service.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core {
namespace {

bool isSuccessful(OperationCode code) {
    return code == OperationCode::Ok ||
        code == OperationCode::PartialSuccess;
}

using NumericSeries = std::map<Timestamp, double>;
using NumericSeriesMap = std::unordered_map<SequenceId, NumericSeries>;

enum class EvaluationStatus {
    Value,
    Missing,
    Error
};

struct EvaluationResult {
    EvaluationStatus status{EvaluationStatus::Missing};
    double value{};
    std::string error;
};

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

bool continuousNumericConfig(const RuntimeInstanceConfig& config) {
    if (config.series_kind != SeriesKind::Continuous) {
        return false;
    }
    return config.data_type == "double" || config.data_type == "float" ||
        config.data_type == "continuous" || config.data_type == "int" ||
        config.data_type == "int64" || config.data_type == "integer";
}

void collectSources(
    const DerivedExpression& expression,
    std::set<SequenceId>* sequence_ids) {
    switch (expression.kind) {
        case DerivedExpression::NodeKind::Sequence:
            sequence_ids->insert(expression.sequence_id);
            return;
        case DerivedExpression::NodeKind::Constant:
            return;
        case DerivedExpression::NodeKind::Binary:
            if (expression.binary.left) {
                collectSources(*expression.binary.left, sequence_ids);
            }
            if (expression.binary.right) {
                collectSources(*expression.binary.right, sequence_ids);
            }
            return;
    }
}

std::set<SequenceId> sourcesFor(
    const RuntimeDerivedSeriesConfig& config) {
    std::set<SequenceId> sequence_ids;
    if (const auto* linear = std::get_if<DerivedLinearCombination>(
            &config.formula);
        linear != nullptr) {
        for (const auto& term : linear->terms) {
            sequence_ids.insert(term.sequence_id);
        }
    } else if (const auto* expression = std::get_if<DerivedExpression>(
            &config.formula);
        expression != nullptr) {
        collectSources(*expression, &sequence_ids);
    }
    return sequence_ids;
}

bool valueAt(
    const NumericSeries& series,
    SeriesKind series_kind,
    Timestamp time,
    double* output) {
    const auto exact = series.find(time);
    if (exact != series.end()) {
        *output = exact->second;
        return true;
    }

    const auto right = series.lower_bound(time);
    if (series_kind == SeriesKind::Continuous) {
        if (right == series.begin() || right == series.end()) {
            return false;
        }
        const auto left = std::prev(right);
        const auto distance = right->first - left->first;
        if (distance <= 0) {
            return false;
        }
        const auto ratio = static_cast<double>(time - left->first) /
            static_cast<double>(distance);
        *output = left->second + (right->second - left->second) * ratio;
        return std::isfinite(*output);
    }

    if (right == series.begin()) {
        return false;
    }
    *output = std::prev(right)->second;
    return true;
}

EvaluationResult evaluateExpression(
    const DerivedExpression& expression,
    Timestamp time,
    const NumericSeriesMap& values,
    const std::unordered_map<SequenceId, SeriesKind>& series_kinds) {
    switch (expression.kind) {
        case DerivedExpression::NodeKind::Sequence: {
            const auto values_it = values.find(expression.sequence_id);
            const auto kinds_it = series_kinds.find(expression.sequence_id);
            if (values_it == values.end() || kinds_it == series_kinds.end()) {
                return {EvaluationStatus::Missing, 0.0, {}};
            }
            double value = 0.0;
            if (!valueAt(
                    values_it->second, kinds_it->second, time, &value)) {
                return {EvaluationStatus::Missing, 0.0, {}};
            }
            return {EvaluationStatus::Value, value, {}};
        }
        case DerivedExpression::NodeKind::Constant:
            return {EvaluationStatus::Value, expression.constant, {}};
        case DerivedExpression::NodeKind::Binary:
            if (!expression.binary.left || !expression.binary.right) {
                return {
                    EvaluationStatus::Error,
                    0.0,
                    "derived binary expression is incomplete"};
            }
            {
                const auto left = evaluateExpression(
                    *expression.binary.left, time, values, series_kinds);
                const auto right = evaluateExpression(
                    *expression.binary.right, time, values, series_kinds);
                if (left.status == EvaluationStatus::Error) {
                    return left;
                }
                if (right.status == EvaluationStatus::Error) {
                    return right;
                }
                if (left.status == EvaluationStatus::Missing ||
                    right.status == EvaluationStatus::Missing) {
                    return {EvaluationStatus::Missing, 0.0, {}};
                }

                double value = 0.0;
                switch (expression.binary.operation) {
                    case DerivedOperator::Add:
                        value = left.value + right.value;
                        break;
                    case DerivedOperator::Subtract:
                        value = left.value - right.value;
                        break;
                    case DerivedOperator::Multiply:
                        value = left.value * right.value;
                        break;
                    case DerivedOperator::Divide:
                        if (right.value == 0.0) {
                            return {
                                EvaluationStatus::Error,
                                0.0,
                                "derived division by zero"};
                        }
                        value = left.value / right.value;
                        break;
                    case DerivedOperator::Unspecified:
                        return {
                            EvaluationStatus::Error,
                            0.0,
                            "derived operator is unspecified"};
                }
                if (!std::isfinite(value)) {
                    return {
                        EvaluationStatus::Error,
                        0.0,
                        "derived operation produced a non-finite value"};
                }
                return {EvaluationStatus::Value, value, {}};
            }
    }
    return {EvaluationStatus::Error, 0.0, "unknown derived expression node"};
}

EvaluationResult evaluateFormula(
    const RuntimeDerivedSeriesConfig& config,
    Timestamp time,
    const NumericSeriesMap& values,
    const std::unordered_map<SequenceId, SeriesKind>& series_kinds) {
    if (const auto* linear = std::get_if<DerivedLinearCombination>(
            &config.formula);
        linear != nullptr) {
        double value = linear->bias;
        for (const auto& term : linear->terms) {
            const auto values_it = values.find(term.sequence_id);
            const auto kinds_it = series_kinds.find(term.sequence_id);
            if (values_it == values.end() || kinds_it == series_kinds.end()) {
                return {EvaluationStatus::Missing, 0.0, {}};
            }
            double source_value = 0.0;
            if (!valueAt(
                    values_it->second, kinds_it->second,
                    time, &source_value)) {
                return {EvaluationStatus::Missing, 0.0, {}};
            }
            value += term.coefficient * source_value;
        }
        if (!std::isfinite(value)) {
            return {
                EvaluationStatus::Error,
                0.0,
                "derived linear combination produced a non-finite value"};
        }
        return {EvaluationStatus::Value, value, {}};
    }

    if (const auto* expression = std::get_if<DerivedExpression>(
            &config.formula);
        expression != nullptr) {
        return evaluateExpression(*expression, time, values, series_kinds);
    }
    return {EvaluationStatus::Error, 0.0, "derived formula is not set"};
}

}  // namespace

OperationResult DerivedSeriesService::refresh() {
    return refreshInternal(nullptr);
}

OperationResult DerivedSeriesService::refresh(
    const WindowUpdateResult& update) {
    return refreshInternal(&update);
}

OperationResult DerivedSeriesService::refreshInternal(
    const WindowUpdateResult* update) {
    if (update != nullptr && !isSuccessful(update->operation.code)) {
        return update->operation;
    }
    const bool incremental = update != nullptr &&
        update->incremental_safe &&
        update->affected_start_time.has_value() &&
        update->affected_end_time.has_value();

    std::lock_guard lock(refresh_mutex_);
    const auto configs = configs_.allDerivedSeries();
    std::vector<SequenceId> changed_sequence_ids;
    if (update != nullptr) {
        changed_sequence_ids = update->changed_sequence_ids;
    }
    std::size_t success_count = 0;
    std::size_t failed_count = 0;
    std::string first_error;

    const auto changed = [&changed_sequence_ids](
                             const std::set<SequenceId>& source_ids) {
        return std::any_of(
            source_ids.begin(), source_ids.end(),
            [&changed_sequence_ids](const SequenceId& source_id) {
                return std::find(
                    changed_sequence_ids.begin(),
                    changed_sequence_ids.end(),
                    source_id) != changed_sequence_ids.end();
            });
    };

    for (const auto& config : configs) {
        if (!config.enabled) {
            if (incremental) {
                // A disabled configuration has no dependency to update.
                // Configuration synchronization uses refresh() and clears it
                // through the full path when necessary.
                continue;
            }
            const auto cleared = window_service_.replaceDerivedSequence(
                config.derived_sequence_id, {});
            if (cleared.code != OperationCode::Ok && first_error.empty()) {
                first_error = cleared.message;
            }
            continue;
        }

        const auto source_ids = sourcesFor(config);
        if (incremental && !changed(source_ids)) {
            continue;
        }
        WindowQuery query;
        query.sequence_ids.assign(source_ids.begin(), source_ids.end());
        const auto window_result = window_service_.queryWindowData(query);
        if (window_result.operation.code != OperationCode::Ok) {
            ++failed_count;
            if (first_error.empty()) {
                first_error = window_result.operation.message;
            }
            continue;
        }

        NumericSeriesMap values;
        std::unordered_map<SequenceId, SeriesKind> series_kinds;
        std::set<Timestamp> timestamps;
        bool locally_incremental = incremental;
        for (const auto& source_id : source_ids) {
            const auto config_it = configs_.findInstance(source_id);
            if (!config_it) {
                locally_incremental = false;
                ++failed_count;
                if (first_error.empty()) {
                    first_error = "derived source sequence is not registered: " +
                        source_id;
                }
                continue;
            }
            if (!continuousNumericConfig(*config_it)) {
                // Discrete/nearest-value sources may change every later
                // derived timestamp after a new point, so rebuild safely.
                locally_incremental = false;
                ++failed_count;
                if (first_error.empty()) {
                    first_error =
                        "derived source is no longer a continuous numeric "
                        "sequence: " + source_id;
                }
                continue;
            }
            series_kinds.emplace(source_id, config_it->series_kind);
            const auto points_it = window_result.data.sequence_values.find(
                source_id);
            if (points_it == window_result.data.sequence_values.end()) {
                values.emplace(source_id, NumericSeries{});
                continue;
            }
            auto& numeric = values[source_id];
            for (const auto& point : points_it->second) {
                double value = 0.0;
                if (!numericValue(point.value, &value)) {
                    ++failed_count;
                    if (first_error.empty()) {
                        first_error =
                            "derived source contains a non-finite numeric value";
                    }
                    continue;
                }
                numeric[point.time] = value;
                timestamps.insert(point.time);
            }
        }

        Timestamp patch_start = window_result.data.window_start_time;
        Timestamp patch_end = window_result.data.window_end_time ==
                std::numeric_limits<Timestamp>::min()
            ? window_result.data.window_end_time
            : window_result.data.window_end_time - 1;
        if (locally_incremental) {
            patch_start = *update->affected_start_time;
            patch_end = *update->affected_end_time;
            // A new right endpoint can change interpolation values at the
            // preceding source point, so include one predecessor per source.
            for (const auto& [source_id, source_points] :
                 window_result.data.sequence_values) {
                (void)source_id;
                const auto first = std::lower_bound(
                    source_points.begin(), source_points.end(), patch_start,
                    [](const RawTimeseriesPoint& point, Timestamp time) {
                        return point.time < time;
                    });
                if (first != source_points.begin()) {
                    patch_start = std::min(patch_start, (first - 1)->time);
                } else if (first != source_points.end()) {
                    patch_start = std::min(patch_start, first->time);
                }
            }
        }

        TimeseriesBatch derived;
        derived.points.reserve(timestamps.size());
        std::size_t expression_errors = 0;
        for (const auto time : timestamps) {
            if (locally_incremental &&
                (time < patch_start || time > patch_end)) {
                continue;
            }
            const auto evaluation = evaluateFormula(
                config, time, values, series_kinds);
            if (evaluation.status == EvaluationStatus::Missing) {
                continue;
            }
            if (evaluation.status == EvaluationStatus::Error) {
                ++expression_errors;
                if (first_error.empty()) {
                    first_error = evaluation.error;
                }
                continue;
            }
            derived.points.push_back({
                time, config.derived_sequence_id, evaluation.value});
        }
        failed_count += expression_errors;
        const auto replaced = locally_incremental
            ? window_service_.patchDerivedSequence(
                  config.derived_sequence_id,
                  patch_start,
                  patch_end,
                  derived)
            : window_service_.replaceDerivedSequence(
                  config.derived_sequence_id, derived);
        if (replaced.code != OperationCode::Ok) {
            ++failed_count;
            if (first_error.empty()) {
                first_error = replaced.message;
            }
        }
        success_count += derived.points.size();
        if (update != nullptr &&
            std::find(
                changed_sequence_ids.begin(),
                changed_sequence_ids.end(),
                config.derived_sequence_id) == changed_sequence_ids.end()) {
            changed_sequence_ids.push_back(config.derived_sequence_id);
        }
    }

    if (failed_count != 0) {
        return internal::makeOperationResult(
            OperationCode::PartialSuccess,
            success_count,
            failed_count,
            "derived hot windows refreshed with partial failures: " +
                first_error);
    }
    return internal::ok(success_count, "derived hot windows refreshed");
}

}  // namespace sfkg::timeseries::core
