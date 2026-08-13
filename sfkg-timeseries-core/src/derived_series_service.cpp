#include "sfkg/timeseries/core/derived_series_service.hpp"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <future>
#include <limits>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core {
namespace {

bool isSuccessful(OperationCode code) {
    return code == OperationCode::Ok ||
        code == OperationCode::PartialSuccess;
}

// WindowService already returns each sequence in timestamp order. Keep that
// layout instead of copying it into a tree: the monotonic cursor below still
// supports irregular timestamps, while avoiding one tree node and allocator
// operation per point.
struct NumericPoint {
    Timestamp time{0};
    double value{0.0};
};

using NumericSeries = std::vector<NumericPoint>;

struct NumericSource {
    NumericSeries points;
    SeriesKind kind{SeriesKind::Unspecified};
    std::size_t invalid_value_count{0};
};

using NumericSeriesMap = std::unordered_map<SequenceId, NumericSource>;

struct NumericSnapshot {
    NumericSeriesMap values;
};

struct NumericCursor {
    const NumericSource* source{nullptr};
    std::size_t right_index{0};
};

using NumericCursorMap = std::unordered_map<SequenceId, NumericCursor>;

struct IncrementalRefreshRange {
    Timestamp start_time{0};
    Timestamp end_time{0};
    bool window_evicted{false};
};

// Resolve incremental safety for exactly the dependency set of one derived
// formula. The aggregate WindowUpdateResult::incremental_safe is retained for
// diagnostics/backward compatibility, but must not make an unrelated source
// force this formula through a full refresh.
std::optional<IncrementalRefreshRange> incrementalRangeFor(
    const WindowUpdateResult* update,
    const std::vector<SequenceId>& sequence_ids) {
    if (update == nullptr || sequence_ids.empty()) {
        return std::nullopt;
    }

    // Keep compatibility with manually constructed WindowUpdateResult values
    // used by older callers/tests before per-sequence state was introduced.
    if (update->sequence_updates.empty()) {
        if (!update->incremental_safe ||
            !update->affected_start_time ||
            !update->affected_end_time) {
            return std::nullopt;
        }
        auto range = IncrementalRefreshRange{
            *update->affected_start_time,
            *update->affected_end_time,
            update->window_evicted};
        if (range.window_evicted && update->window_start_time) {
            range.start_time = std::min(
                range.start_time, *update->window_start_time);
        }
        return range;
    }

    std::optional<IncrementalRefreshRange> range;
    for (const auto& sequence_id : sequence_ids) {
        if (std::find(
                update->changed_sequence_ids.begin(),
                update->changed_sequence_ids.end(),
                sequence_id) == update->changed_sequence_ids.end()) {
            // An unchanged dependency is still queried as context, but it
            // does not make the formula's affected range unsafe.
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
            range = IncrementalRefreshRange{
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
    std::vector<SequenceId>* sequence_ids,
    std::unordered_set<SequenceId>* seen) {
    switch (expression.kind) {
        case DerivedExpression::NodeKind::Sequence:
            if (seen->insert(expression.sequence_id).second) {
                sequence_ids->push_back(expression.sequence_id);
            }
            return;
        case DerivedExpression::NodeKind::Constant:
            return;
        case DerivedExpression::NodeKind::Binary:
            if (expression.binary.left) {
                collectSources(*expression.binary.left, sequence_ids, seen);
            }
            if (expression.binary.right) {
                collectSources(*expression.binary.right, sequence_ids, seen);
            }
            return;
    }
}

std::vector<SequenceId> sourcesFor(
    const RuntimeDerivedSeriesConfig& config) {
    std::vector<SequenceId> sequence_ids;
    std::unordered_set<SequenceId> seen;
    if (const auto* linear = std::get_if<DerivedLinearCombination>(
            &config.formula);
        linear != nullptr) {
        for (const auto& term : linear->terms) {
            if (seen.insert(term.sequence_id).second) {
                sequence_ids.push_back(term.sequence_id);
            }
        }
    } else if (const auto* expression = std::get_if<DerivedExpression>(
            &config.formula);
        expression != nullptr) {
        collectSources(*expression, &sequence_ids, &seen);
    }
    return sequence_ids;
}

bool valueAt(
    NumericCursor& cursor,
    Timestamp time,
    double* output) {
    if (cursor.source == nullptr) {
        return false;
    }
    const auto& series = cursor.source->points;
    while (cursor.right_index < series.size() &&
           series[cursor.right_index].time < time) {
        ++cursor.right_index;
    }
    if (cursor.right_index < series.size() &&
        series[cursor.right_index].time == time) {
        *output = series[cursor.right_index].value;
        return true;
    }

    const auto right = series.begin() +
        static_cast<std::ptrdiff_t>(cursor.right_index);
    const auto series_kind = cursor.source->kind;
    if (series_kind == SeriesKind::Continuous) {
        if (right == series.begin() || right == series.end()) {
            return false;
        }
        const auto left = std::prev(right);
        const auto distance = right->time - left->time;
        if (distance <= 0) {
            return false;
        }
        const auto ratio = static_cast<double>(time - left->time) /
            static_cast<double>(distance);
        *output = left->value + (right->value - left->value) * ratio;
        return std::isfinite(*output);
    }

    if (right == series.begin()) {
        return false;
    }
    *output = std::prev(right)->value;
    return true;
}

EvaluationResult evaluateExpression(
    const DerivedExpression& expression,
    Timestamp time,
    NumericCursorMap& values) {
    switch (expression.kind) {
        case DerivedExpression::NodeKind::Sequence: {
            const auto values_it = values.find(expression.sequence_id);
            if (values_it == values.end()) {
                return {EvaluationStatus::Missing, 0.0, {}};
            }
            double value = 0.0;
            if (!valueAt(
                    values_it->second,
                    time,
                    &value)) {
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
                    *expression.binary.left, time, values);
                const auto right = evaluateExpression(
                    *expression.binary.right, time, values);
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
    NumericCursorMap& values) {
    if (const auto* linear = std::get_if<DerivedLinearCombination>(
            &config.formula);
        linear != nullptr) {
        double value = linear->bias;
        for (const auto& term : linear->terms) {
            const auto values_it = values.find(term.sequence_id);
            if (values_it == values.end()) {
                return {EvaluationStatus::Missing, 0.0, {}};
            }
            double source_value = 0.0;
            if (!valueAt(
                    values_it->second,
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
        return evaluateExpression(*expression, time, values);
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
    std::unique_lock<std::mutex> full_refresh_lock(
        full_refresh_mutex_, std::defer_lock);
    if (update == nullptr) {
        full_refresh_lock.lock();
    }
    const auto configs = configs_.allDerivedSeries();
    std::vector<SequenceId> changed_sequence_ids;
    if (update != nullptr) {
        changed_sequence_ids = update->changed_sequence_ids;
    }
    struct DerivedRefreshItem {
        SequenceId sequence_id;
        TimeseriesBatch points;
        Timestamp patch_start{0};
        Timestamp patch_end{0};
        bool skip{false};
        bool publish{false};
        bool clear_only{false};
        bool incremental{false};
        std::size_t failed_count{0};
        std::string first_error;
    };

    const auto changed = [&changed_sequence_ids](
                             const std::vector<SequenceId>& source_ids) {
        return std::any_of(
            source_ids.begin(), source_ids.end(),
            [&changed_sequence_ids](const SequenceId& source_id) {
                return std::find(
                    changed_sequence_ids.begin(),
                    changed_sequence_ids.end(),
                    source_id) != changed_sequence_ids.end();
            });
    };

    std::unordered_map<
        SequenceId,
        std::optional<RuntimeInstanceConfig>> source_configs;
    std::vector<SequenceId> incremental_source_ids;
    std::vector<SequenceId> full_source_ids;
    std::unordered_set<SequenceId> incremental_seen;
    std::unordered_set<SequenceId> full_seen;
    std::optional<IncrementalRefreshRange> incremental_query_range;
    const auto mergeIncrementalRange = [&incremental_query_range](
                                            const IncrementalRefreshRange& range) {
        if (!incremental_query_range) {
            incremental_query_range = range;
            return;
        }
        incremental_query_range->start_time = std::min(
            incremental_query_range->start_time, range.start_time);
        incremental_query_range->end_time = std::max(
            incremental_query_range->end_time, range.end_time);
        incremental_query_range->window_evicted =
            incremental_query_range->window_evicted || range.window_evicted;
    };
    const auto addUnique = [](
                                const SequenceId& sequence_id,
                                std::vector<SequenceId>* output,
                                std::unordered_set<SequenceId>* seen) {
        if (seen->insert(sequence_id).second) {
            output->push_back(sequence_id);
        }
    };

    // Cache registry lookups and build the union of source sequences for each
    // refresh mode.  This lets all incremental derived configs share one
    // bounded WindowService query, while unsupported/discrete configs retain
    // their safe full-window path.
    for (const auto& config : configs) {
        if (!config.enabled) {
            continue;
        }
        const auto source_ids = sourcesFor(config);
        if (update != nullptr && !changed(source_ids)) {
            continue;
        }
        const auto source_range = incrementalRangeFor(update, source_ids);
        bool use_incremental = source_range.has_value();
        for (const auto& source_id : source_ids) {
            const auto [source_it, inserted] = source_configs.emplace(
                source_id, configs_.findInstance(source_id));
            (void)inserted;
            if (use_incremental &&
                (!source_it->second ||
                 !continuousNumericConfig(*source_it->second))) {
                use_incremental = false;
            }
        }
        for (const auto& source_id : source_ids) {
            addUnique(
                source_id,
                use_incremental ? &incremental_source_ids : &full_source_ids,
                use_incremental ? &incremental_seen : &full_seen);
        }
        if (use_incremental && source_range) {
            mergeIncrementalRange(*source_range);
        }
    }

    const auto buildQuery = [](
                                  const std::vector<SequenceId>& sequence_ids,
                                  const std::optional<IncrementalRefreshRange>&
                                      range) {
        WindowQuery query;
        query.sequence_ids = sequence_ids;
        if (range) {
            query.start_time = range->start_time;
            query.end_time = range->end_time ==
                    std::numeric_limits<Timestamp>::max()
                ? range->end_time
                : range->end_time + 1;
            query.preceding_points = 1;
            query.following_points = 1;
        }
        return query;
    };

    WindowQueryResult incremental_window;
    WindowQueryResult full_window;
    const bool has_incremental_window = !incremental_source_ids.empty();
    const bool has_full_window = !full_source_ids.empty();
    if (has_incremental_window) {
        incremental_window = window_service_.queryWindowData(
            buildQuery(incremental_source_ids, incremental_query_range));
    }
    if (has_full_window) {
        full_window = window_service_.queryWindowData(
            buildQuery(full_source_ids, std::nullopt));
    }

    const auto buildNumericSnapshot = [&source_configs](
                                           const WindowQueryResult& window,
                                           const std::vector<SequenceId>&
                                               source_ids) {
        NumericSnapshot snapshot;
        for (const auto& source_id : source_ids) {
            const auto config_it = source_configs.find(source_id);
            if (config_it == source_configs.end() || !config_it->second ||
                !continuousNumericConfig(*config_it->second)) {
                continue;
            }
            NumericSource numeric;
            numeric.kind = config_it->second->series_kind;
            const auto points_it = window.data.sequence_values.find(source_id);
            if (points_it != window.data.sequence_values.end()) {
                numeric.points.reserve(points_it->second.size());
                for (const auto& point : points_it->second) {
                    double value = 0.0;
                    if (!numericValue(point.value, &value)) {
                        ++numeric.invalid_value_count;
                        continue;
                    }
                    numeric.points.push_back({point.time, value});
                }
            }
            snapshot.values.emplace(source_id, std::move(numeric));
        }
        return snapshot;
    };

    NumericSnapshot incremental_values;
    NumericSnapshot full_values;
    const bool incremental_snapshot_ready =
        has_incremental_window &&
        incremental_window.operation.code == OperationCode::Ok;
    const bool full_snapshot_ready =
        has_full_window && full_window.operation.code == OperationCode::Ok;
    if (incremental_snapshot_ready) {
        incremental_values = buildNumericSnapshot(
            incremental_window, incremental_source_ids);
    }
    if (full_snapshot_ready) {
        full_values = buildNumericSnapshot(full_window, full_source_ids);
    }

    const auto computeOne = [
        &source_configs,
        &incremental_window,
        &full_window,
        &incremental_values,
        &full_values,
        has_incremental_window,
        has_full_window,
        incremental_snapshot_ready,
        full_snapshot_ready,
        &changed,
        update](
                                const RuntimeDerivedSeriesConfig& config) {
        DerivedRefreshItem item;
        item.sequence_id = config.derived_sequence_id;
        if (!config.enabled) {
            if (update != nullptr) {
                // A disabled configuration has no dependency to update.
                // Configuration synchronization uses refresh() and clears it
                // through the full path when necessary.
                item.skip = true;
                return item;
            }
            item.publish = true;
            item.clear_only = true;
            return item;
        }

        const auto source_ids = sourcesFor(config);
        if (update != nullptr && !changed(source_ids)) {
            item.skip = true;
            return item;
        }

        // A range refresh is safe only for registered continuous numeric
        // sources. Discrete/nearest-value sources can change every later
        // derived timestamp, so they must use the full-window path.
        const auto item_range = incrementalRangeFor(update, source_ids);
        item.incremental = item_range.has_value();
        if (item.incremental) {
            for (const auto& source_id : source_ids) {
                const auto source_config = source_configs.find(source_id);
                if (source_config == source_configs.end() ||
                    !source_config->second ||
                    !continuousNumericConfig(*source_config->second)) {
                    item.incremental = false;
                    break;
                }
            }
        }

        const auto* window_result = item.incremental
            ? (has_incremental_window ? &incremental_window : nullptr)
            : (has_full_window ? &full_window : nullptr);
        if (window_result == nullptr ||
            window_result->operation.code != OperationCode::Ok) {
            item.failed_count = 1;
            item.first_error = window_result == nullptr
                ? "derived source query was not prepared"
                : window_result->operation.message;
            return item;
        }
        const auto& window_data = window_result->data;
        const auto* numeric_snapshot = item.incremental
            ? (incremental_snapshot_ready ? &incremental_values : nullptr)
            : (full_snapshot_ready ? &full_values : nullptr);
        if (numeric_snapshot == nullptr) {
            item.failed_count = 1;
            item.first_error = "derived numeric snapshot was not prepared";
            return item;
        }

        const auto& values = numeric_snapshot->values;
        std::vector<Timestamp> timestamps;
        for (const auto& source_id : source_ids) {
            const auto config_it = source_configs.find(source_id);
            if (config_it == source_configs.end() || !config_it->second) {
                item.incremental = false;
                ++item.failed_count;
                if (item.first_error.empty()) {
                    item.first_error =
                        "derived source sequence is not registered: " +
                        source_id;
                }
                continue;
            }
            const auto& source_config = *config_it->second;
            if (!continuousNumericConfig(source_config)) {
                // Discrete/nearest-value sources may change every later
                // derived timestamp after a new point, so rebuild safely.
                item.incremental = false;
                ++item.failed_count;
                if (item.first_error.empty()) {
                    item.first_error =
                        "derived source is no longer a continuous numeric "
                        "sequence: " + source_id;
                }
                continue;
            }
            const auto points_it = values.find(source_id);
            if (points_it == values.end()) {
                continue;
            }
            if (points_it->second.invalid_value_count != 0) {
                item.failed_count += points_it->second.invalid_value_count;
                if (item.first_error.empty()) {
                    item.first_error =
                        "derived source contains a non-finite numeric value";
                }
            }
            for (const auto& point : points_it->second.points) {
                timestamps.push_back(point.time);
            }
            /*
             * Numeric conversion is performed once in the shared snapshot.
             * The remaining loop only collects candidate timestamps for this
             * formula, so configs using different subsets do not evaluate
             * unrelated source timestamps.
             */
        }

        std::sort(timestamps.begin(), timestamps.end());
        timestamps.erase(
            std::unique(timestamps.begin(), timestamps.end()),
            timestamps.end());

        // Formula timestamps are traversed in ascending order. Keep one
        // monotonic cursor per source so repeated interpolation does not do a
        // lower_bound/tree lookup for every derived point.
        NumericCursorMap cursors;
        cursors.reserve(source_ids.size());
        for (const auto& source_id : source_ids) {
            const auto values_it = values.find(source_id);
            if (values_it != values.end()) {
                cursors.emplace(
                    source_id,
                    NumericCursor{&values_it->second, 0});
            }
        }

        Timestamp patch_start = 0;
        Timestamp patch_end = 0;
        if (!item.incremental) {
            patch_start = window_data.window_start_time;
            patch_end = window_data.window_end_time ==
                    std::numeric_limits<Timestamp>::min()
                ? window_data.window_end_time
                : window_data.window_end_time - 1;
        } else {
            patch_start = item_range->start_time;
            patch_end = item_range->end_time;
            // The query includes one predecessor per source. A new right
            // endpoint can change interpolation at that predecessor, so the
            // patch begins at the earliest returned point before the affected
            // range. The following point is context only and is not patched.
            for (const auto& source_id : source_ids) {
                const auto points_it = window_data.sequence_values.find(
                    source_id);
                if (points_it == window_data.sequence_values.end()) {
                    continue;
                }
                const auto& source_points = points_it->second;
                const auto first = std::lower_bound(
                    source_points.begin(), source_points.end(),
                    item_range->start_time,
                    [](const RawTimeseriesPoint& point, Timestamp time) {
                        return point.time < time;
                    });
                if (first != source_points.begin()) {
                    patch_start = std::min(patch_start, (first - 1)->time);
                }
            }
        }

        TimeseriesBatch derived;
        derived.points.reserve(timestamps.size());
        std::size_t expression_errors = 0;
        for (const auto time : timestamps) {
            if (item.incremental &&
                (time < patch_start || time > patch_end)) {
                continue;
            }
            const auto evaluation = evaluateFormula(config, time, cursors);
            if (evaluation.status == EvaluationStatus::Missing) {
                continue;
            }
            if (evaluation.status == EvaluationStatus::Error) {
                ++expression_errors;
                if (item.first_error.empty()) {
                    item.first_error = evaluation.error;
                }
                continue;
            }
            derived.points.push_back({
                time, config.derived_sequence_id, evaluation.value});
        }
        item.failed_count += expression_errors;
        item.points.points = std::move(derived.points);
        item.patch_start = patch_start;
        item.patch_end = patch_end;
        item.publish = true;
        return item;
    };

    // Current registry validation requires every derived source to be a raw
    // instance, so independent derived configurations can be computed in
    // parallel. Keep this bounded: refresh() itself may already be running
    // on several HOT workers, and unbounded async fan-out would oversubscribe
    // the process. Results are published below in one serialized phase.
    std::vector<DerivedRefreshItem> computed(configs.size());
    if (configs.size() <= 1) {
        if (!configs.empty()) {
            computed.front() = computeOne(configs.front());
        }
    } else {
        const auto hardware_threads = std::thread::hardware_concurrency();
        const auto worker_count = std::min<std::size_t>(
            configs.size(),
            std::min<std::size_t>(4, hardware_threads == 0 ? 1 : hardware_threads));
        std::atomic<std::size_t> next_config{0};
        std::vector<std::future<void>> workers;
        workers.reserve(worker_count);
        for (std::size_t worker = 0; worker < worker_count; ++worker) {
            workers.push_back(std::async(
                std::launch::async,
                [&] {
                    while (true) {
                        const auto index = next_config.fetch_add(
                            1, std::memory_order_relaxed);
                        if (index >= configs.size()) {
                            return;
                        }
                        computed[index] = computeOne(configs[index]);
                    }
                }));
        }
        for (auto& worker : workers) {
            worker.get();
        }
    }

    std::size_t success_count = 0;
    std::size_t failed_count = 0;
    std::string first_error;
    for (auto& item : computed) {
        if (item.skip) {
            continue;
        }
        if (!item.publish) {
            failed_count += item.failed_count;
            if (first_error.empty() && !item.first_error.empty()) {
                first_error = item.first_error;
            }
            continue;
        }
        if (!item.sequence_id.empty() && item.clear_only) {
            std::lock_guard publish_lock(publish_mutex_);
            const auto cleared = window_service_.replaceDerivedSequence(
                item.sequence_id, {});
            if (cleared.code != OperationCode::Ok) {
                ++failed_count;
                if (first_error.empty()) {
                    first_error = cleared.message;
                }
            }
        } else if (!item.sequence_id.empty()) {
            OperationResult published;
            {
                std::lock_guard publish_lock(publish_mutex_);
                if (item.incremental && update != nullptr &&
                    update->update_generation != 0 &&
                    update->update_generation < last_published_generation_) {
                    published = internal::ok(
                        0,
                        "stale derived refresh skipped after newer window "
                        "update");
                } else {
                    if (item.incremental && update != nullptr) {
                        last_published_generation_ = std::max(
                            last_published_generation_,
                            update->update_generation);
                    }
                    published = item.incremental
                        ? window_service_.patchDerivedSequence(
                              item.sequence_id,
                              item.patch_start,
                              item.patch_end,
                              item.points)
                        : window_service_.replaceDerivedSequence(
                              item.sequence_id, item.points);
                }
            }
            if (published.code != OperationCode::Ok) {
                ++failed_count;
                if (first_error.empty()) {
                    first_error = published.message;
                }
            }
            success_count += item.points.points.size();
        }
        failed_count += item.failed_count;
        if (first_error.empty() && !item.first_error.empty()) {
            first_error = item.first_error;
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
