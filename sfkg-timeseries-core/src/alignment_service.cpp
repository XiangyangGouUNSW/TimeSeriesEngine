#include "sfkg/timeseries/core/alignment_service.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <iterator>
#include <limits>
#include <optional>
#include <future>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "operation_helpers.hpp"
#include "sfkg/timeseries/core/runtime_config_registry.hpp"

namespace sfkg::timeseries::core {
namespace {

using BucketValues = std::vector<std::optional<TimeseriesValue>>;

bool numericValue(const TimeseriesValue& value, double* output);

struct BucketAccumulator {
    bool has_value{false};
    Timestamp first_time{};
    Timestamp last_time{};
    TimeseriesValue first_value;
    TimeseriesValue last_value;
    double sum{0.0};
    double minimum{0.0};
    double maximum{0.0};
    std::size_t count{0};

    bool add(
        const RawTimeseriesPoint& point,
        BucketAggregation aggregation,
        std::string* error) {
        const bool was_empty = !has_value;
        if (aggregation != BucketAggregation::First &&
            aggregation != BucketAggregation::Last) {
            double value = 0.0;
            if (!numericValue(point.value, &value)) {
                *error =
                    "numeric aggregation requires finite numeric sequence values";
                return false;
            }
            if (!has_value) {
                minimum = value;
                maximum = value;
            } else {
                minimum = std::min(minimum, value);
                maximum = std::max(maximum, value);
            }
            sum += value;
            ++count;
            if (aggregation == BucketAggregation::Average &&
                !std::isfinite(sum)) {
                *error = "average aggregation produced a non-finite value";
                return false;
            }
        }

        if (was_empty) {
            has_value = true;
            first_time = point.time;
            last_time = point.time;
            first_value = point.value;
            last_value = point.value;
            return true;
        }
        // Points are normally ordered, but comparing timestamps preserves the
        // First/Last semantics even for a manually constructed window.
        if (point.time < first_time) {
            first_time = point.time;
            first_value = point.value;
        }
        if (point.time > last_time) {
            last_time = point.time;
            last_value = point.value;
        }
        return true;
    }

    bool finish(
        BucketAggregation aggregation,
        TimeseriesValue* output,
        std::string* error) const {
        if (!has_value) {
            return false;
        }
        switch (aggregation) {
            case BucketAggregation::First:
                *output = first_value;
                return true;
            case BucketAggregation::Last:
                *output = last_value;
                return true;
            case BucketAggregation::Average: {
                const auto average = sum / static_cast<double>(count);
                if (!std::isfinite(average)) {
                    *error = "average aggregation produced a non-finite value";
                    return false;
                }
                *output = average;
                return true;
            }
            case BucketAggregation::Maximum:
                *output = maximum;
                return true;
            case BucketAggregation::Minimum:
                *output = minimum;
                return true;
        }
        *error = "unknown bucket aggregation";
        return false;
    }
};

struct SequenceAlignmentResult {
    BucketValues values;
    std::size_t missing_values{0};
    std::string error;
    bool success{true};
};

BucketAggregation defaultAggregation(SeriesKind kind) {
    switch (kind) {
        case SeriesKind::Continuous:
            return BucketAggregation::Average;
        case SeriesKind::Discrete:
        case SeriesKind::Categorical:
            return BucketAggregation::Last;
        case SeriesKind::Unspecified:
            return BucketAggregation::First;
    }
    return BucketAggregation::First;
}

GapFillMethod defaultFillMethod(SeriesKind kind) {
    switch (kind) {
        case SeriesKind::Continuous:
            return GapFillMethod::Linear;
        case SeriesKind::Discrete:
        case SeriesKind::Categorical:
            return GapFillMethod::Previous;
        case SeriesKind::Unspecified:
            return GapFillMethod::Near;
    }
    return GapFillMethod::Near;
}

std::optional<std::int64_t> inferBucketInterval(
    const WindowData& window_data,
    const std::vector<SequenceAlignmentConfig>& sequences) {
    std::optional<std::uint64_t> smallest_gap;
    const auto observeGap = [&smallest_gap](
                                Timestamp previous,
                                Timestamp current) {
        // The caller guarantees current >= previous.  Unsigned subtraction
        // also handles a valid range crossing Timestamp zero without signed
        // overflow.
        const auto gap = static_cast<std::uint64_t>(current) -
            static_cast<std::uint64_t>(previous);
        if (gap == 0 || gap > static_cast<std::uint64_t>(
                std::numeric_limits<std::int64_t>::max())) {
            return;
        }
        if (!smallest_gap || gap < *smallest_gap) {
            smallest_gap = gap;
        }
    };

    for (const auto& sequence : sequences) {
        const auto found = window_data.sequence_values.find(
            sequence.sequence_id);
        if (found == window_data.sequence_values.end() ||
            found->second.size() < 2) {
            continue;
        }

        const auto& points = found->second;
        bool sorted = true;
        for (std::size_t index = 1; index < points.size(); ++index) {
            if (points[index].time < points[index - 1].time) {
                sorted = false;
                break;
            }
        }

        if (sorted) {
            for (std::size_t index = 1; index < points.size(); ++index) {
                observeGap(points[index - 1].time, points[index].time);
            }
            continue;
        }

        // Public callers may still construct an unsorted WindowData by hand.
        // Preserve the old behavior for that case, but pay the copy/sort cost
        // only when the input actually needs normalization.
        std::vector<Timestamp> times;
        times.reserve(points.size());
        for (const auto& point : points) {
            times.push_back(point.time);
        }
        std::sort(times.begin(), times.end());
        times.erase(std::unique(times.begin(), times.end()), times.end());
        for (std::size_t index = 1; index < times.size(); ++index) {
            observeGap(times[index - 1], times[index]);
        }
    }

    if (!smallest_gap) {
        return std::nullopt;
    }
    return static_cast<std::int64_t>(*smallest_gap);
}

// Returns true when all points are already ordered by timestamp and belong to
// the sequence map entry. WindowService guarantees both invariants for query
// results; checking them here keeps the public incremental overload equivalent
// to the full alignment path for manually constructed WindowData as well.
bool pointsAreSorted(
    const SequenceId& sequence_id,
    const std::vector<RawTimeseriesPoint>& points) {
    for (std::size_t index = 1; index < points.size(); ++index) {
        if (points[index].time < points[index - 1].time) {
            return false;
        }
    }
    for (const auto& point : points) {
        if (point.sequence_id != sequence_id) {
            return false;
        }
    }
    return true;
}

std::size_t bucketIndexForTime(
    Timestamp time,
    Timestamp window_start,
    Timestamp window_end,
    std::size_t bucket_count,
    std::uint64_t interval) {
    if (time <= window_start) {
        return 0;
    }
    if (time >= window_end) {
        return bucket_count - 1;
    }
    const auto offset = static_cast<std::uint64_t>(time) -
        static_cast<std::uint64_t>(window_start);
    return std::min(
        bucket_count - 1,
        static_cast<std::size_t>(offset / interval));
}

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

bool addTimestamp(
    Timestamp value,
    std::int64_t offset,
    Timestamp* result) {
    if ((offset > 0 && value > std::numeric_limits<Timestamp>::max() - offset) ||
        (offset < 0 && value < std::numeric_limits<Timestamp>::min() - offset)) {
        return false;
    }
    *result = value + offset;
    return true;
}

bool addNonNegativeTimestampOffset(
    Timestamp value,
    std::uint64_t offset,
    Timestamp* result) {
    const auto maximum = static_cast<std::uint64_t>(
        std::numeric_limits<Timestamp>::max());
    if (value >= 0) {
        const auto unsigned_value = static_cast<std::uint64_t>(value);
        if (offset > maximum - unsigned_value) {
            return false;
        }
        *result = value + static_cast<Timestamp>(offset);
        return true;
    }

    // Compute the magnitude without negating Timestamp::min() directly.
    const auto magnitude = static_cast<std::uint64_t>(-(value + 1)) + 1;
    if (offset < magnitude) {
        const auto remaining = magnitude - offset;
        if (remaining == maximum + 1) {
            *result = std::numeric_limits<Timestamp>::min();
        } else {
            *result = -static_cast<Timestamp>(remaining);
        }
        return true;
    }

    const auto positive = offset - magnitude;
    if (positive > maximum) {
        return false;
    }
    *result = static_cast<Timestamp>(positive);
    return true;
}

void nearestValueIndices(
    const BucketValues& values,
    std::vector<std::optional<std::size_t>>* previous,
    std::vector<std::optional<std::size_t>>* next) {
    previous->assign(values.size(), std::nullopt);
    next->assign(values.size(), std::nullopt);

    std::optional<std::size_t> last;
    for (std::size_t index = 0; index < values.size(); ++index) {
        (*previous)[index] = last;
        if (values[index]) {
            last = index;
        }
    }
    last.reset();
    for (std::size_t index = values.size(); index > 0; --index) {
        const auto current = index - 1;
        (*next)[current] = last;
        if (values[current]) {
            last = current;
        }
    }
}

void fillNearAt(
    const BucketValues& source,
    BucketValues* values,
    std::size_t index,
    const std::vector<std::optional<std::size_t>>& previous,
    const std::vector<std::optional<std::size_t>>& next) {
    const auto& previous_index = previous[index];
    const auto& next_index = next[index];
    if (!previous_index && !next_index) {
        return;
    }
    if (!previous_index) {
        (*values)[index] = source[*next_index];
        return;
    }
    if (!next_index) {
        (*values)[index] = source[*previous_index];
        return;
    }

    const auto previous_distance = index - *previous_index;
    const auto next_distance = *next_index - index;
    (*values)[index] = previous_distance <= next_distance
        ? source[*previous_index]
        : source[*next_index];
}

bool fillLinear(BucketValues* values, std::string* error) {
    // LINEAR uses nearest-value filling at both boundaries because there is
    // no pair of samples for extrapolation there.
    const auto first = std::find_if(
        values->begin(), values->end(),
        [](const auto& value) { return value.has_value(); });
    if (first == values->end()) {
        return true;
    }
    const auto last = std::find_if(
        values->rbegin(), values->rend(),
        [](const auto& value) { return value.has_value(); }).base() - 1;
    const auto original = *values;

    std::vector<std::optional<std::size_t>> previous;
    std::vector<std::optional<std::size_t>> next;
    nearestValueIndices(original, &previous, &next);

    for (std::size_t index = 0;
         index < static_cast<std::size_t>(first - values->begin());
         ++index) {
        fillNearAt(original, values, index, previous, next);
    }
    for (std::size_t index = static_cast<std::size_t>(last - values->begin()) + 1;
         index < values->size();
         ++index) {
        fillNearAt(original, values, index, previous, next);
    }

    std::size_t index = static_cast<std::size_t>(first - values->begin());
    while (index < values->size()) {
        if ((*values)[index]) {
            ++index;
            continue;
        }
        const auto left = index - 1;
        std::size_t right = index;
        while (right < values->size() && !(*values)[right]) {
            ++right;
        }
        if (right == values->size()) {
            break;
        }

        double left_value = 0.0;
        double right_value = 0.0;
        if (!numericValue((*values)[left].value(), &left_value) ||
            !numericValue((*values)[right].value(), &right_value)) {
            *error = "linear fill requires finite numeric sequence values";
            return false;
        }
        const auto distance = static_cast<double>(right - left);
        for (std::size_t current = index; current < right; ++current) {
            const auto ratio = static_cast<double>(current - left) / distance;
            (*values)[current] =
                left_value + (right_value - left_value) * ratio;
        }
        index = right;
    }
    return true;
}

bool fillValues(
    BucketValues* values,
    GapFillMethod fill_method,
    std::string* error) {
    switch (fill_method) {
        case GapFillMethod::Near:
            {
            const auto original = *values;
            std::vector<std::optional<std::size_t>> previous;
            std::vector<std::optional<std::size_t>> next;
            nearestValueIndices(original, &previous, &next);
            for (std::size_t index = 0; index < values->size(); ++index) {
                if (!(*values)[index]) {
                    fillNearAt(original, values, index, previous, next);
                }
            }
            return true;
            }
        case GapFillMethod::Previous: {
            std::optional<TimeseriesValue> previous;
            for (auto& value : *values) {
                if (value) {
                    previous = value;
                } else if (previous) {
                    value = previous;
                }
            }
            return true;
        }
        case GapFillMethod::Next: {
            std::optional<TimeseriesValue> next;
            for (auto index = values->size(); index > 0; --index) {
                auto& value = (*values)[index - 1];
                if (value) {
                    next = value;
                } else if (next) {
                    value = next;
                }
            }
            return true;
        }
        case GapFillMethod::Linear:
            return fillLinear(values, error);
    }
    *error = "unknown gap fill method";
    return false;
}

}  // namespace

AlignmentResult AlignmentService::alignWindowData(
    const ProjectId& project_id,
    const WindowData& window_data) const {
    return alignWindowData(project_id, window_data, AlignmentConfig{}, {});
}

AlignmentResult AlignmentService::alignWindowData(
    const WindowData& window_data) const {
    return alignWindowData(
        window_data.project_id.empty() ? ProjectId{"default"} :
            window_data.project_id,
        window_data);
}

AlignmentResult AlignmentService::alignWindowData(
    const ProjectId& project_id,
    const WindowData& window_data,
    const AlignmentConfig& config) const {
    return alignWindowData(project_id, window_data, config, {});
}

AlignmentResult AlignmentService::alignWindowData(
    const WindowData& window_data,
    const AlignmentConfig& config) const {
    return alignWindowData(
        window_data.project_id.empty() ? ProjectId{"default"} :
            window_data.project_id,
        window_data,
        config);
}

AlignmentResult AlignmentService::alignWindowData(
    const ProjectId& project_id,
    const WindowData& window_data,
    const std::vector<RuntimeRelationConfig>& relations) const {
    return alignWindowData(project_id, window_data, AlignmentConfig{}, relations);
}

AlignmentResult AlignmentService::alignWindowData(
    const WindowData& window_data,
    const std::vector<RuntimeRelationConfig>& relations) const {
    return alignWindowData(
        window_data.project_id.empty() ? ProjectId{"default"} :
            window_data.project_id,
        window_data,
        relations);
}

AlignmentResult AlignmentService::alignWindowData(
    const ProjectId& project_id,
    const WindowData& window_data,
    const AlignmentRange& range) const {
    if (range.start_time > range.end_time) {
        AlignmentResult result;
        result.operation = internal::invalidArgument(
            "alignment range start time must not be after end time");
        return result;
    }

    const auto cropFullResult = [&](AlignmentResult result) {
        if (result.operation.code != OperationCode::Ok &&
            result.operation.code != OperationCode::PartialSuccess) {
            return result;
        }
        auto& samples = result.aligned_data.samples;
        const auto first = std::lower_bound(
            samples.begin(), samples.end(), range.start_time,
            [](const AlignedSample& sample, Timestamp time) {
                return sample.time < time;
            });
        const auto last = std::upper_bound(
            samples.begin(), samples.end(), range.end_time,
            [](Timestamp time, const AlignedSample& sample) {
                return time < sample.time;
            });
        const auto first_index = static_cast<std::size_t>(
            first - samples.begin());
        const auto prefix = std::min(range.prefix_samples, first_index);
        const auto begin = first - static_cast<std::ptrdiff_t>(prefix);
        samples = std::vector<AlignedSample>(
            std::make_move_iterator(begin),
            std::make_move_iterator(last));
        result.operation = internal::ok(
            samples.size(), "affected window data aligned");
        return result;
    };

    // This overload is used by continuous constraint checks.  Do not build
    // every bucket in the live window and crop it afterwards: retain only the
    // affected bucket interval, the requested aligned prefix, and one bucket
    // on either side for boundary fill/interpolation.
    AlignmentConfig config;
    config.sequences.reserve(window_data.sequence_values.size());
    for (const auto& [sequence_id, points] : window_data.sequence_values) {
        (void)points;
        config.sequences.push_back({
            sequence_id, VariableRole::Independent, std::nullopt, std::nullopt});
    }
    const auto interval = inferBucketInterval(window_data, config.sequences);
    if (!interval || *interval <= 0) {
        AlignmentResult result;
        result.operation = internal::invalidArgument(
            "cannot infer bucket_interval for incremental alignment");
        return result;
    }
    config.bucket_interval = *interval;

    // WindowService query results are ordered.  If a public caller supplies
    // an unsorted window, keep the previous full-alignment behavior instead of
    // applying lower_bound to invalid ranges.
    for (const auto& [sequence_id, points] : window_data.sequence_values) {
        if (!pointsAreSorted(sequence_id, points)) {
            return cropFullResult(alignWindowData(project_id, window_data));
        }
    }

    const auto span = static_cast<std::uint64_t>(
        window_data.window_end_time) -
        static_cast<std::uint64_t>(window_data.window_start_time);
    const auto interval_u64 = static_cast<std::uint64_t>(*interval);
    const auto bucket_count_u64 = span / interval_u64 +
        (span % interval_u64 != 0 ? 1 : 0);
    if (bucket_count_u64 == 0 || bucket_count_u64 >
            std::numeric_limits<std::size_t>::max()) {
        AlignmentResult result;
        result.operation = internal::invalidArgument(
            "incremental alignment window contains no valid buckets");
        return result;
    }
    const auto bucket_count = static_cast<std::size_t>(bucket_count_u64);
    const auto affected_first = bucketIndexForTime(
        range.start_time,
        window_data.window_start_time,
        window_data.window_end_time,
        bucket_count,
        interval_u64);
    const auto affected_last = bucketIndexForTime(
        range.end_time,
        window_data.window_start_time,
        window_data.window_end_time,
        bucket_count,
        interval_u64);
    const auto target_first = affected_first > range.prefix_samples
        ? affected_first - range.prefix_samples
        : 0;
    const auto target_last = std::max(affected_first, affected_last);
    const auto local_first = target_first == 0 ? 0 : target_first - 1;
    const auto local_last = target_last + 1 < bucket_count
        ? target_last + 1
        : bucket_count - 1;

    Timestamp local_start = window_data.window_start_time;
    if (!addNonNegativeTimestampOffset(
            window_data.window_start_time,
            static_cast<std::uint64_t>(local_first) * interval_u64,
            &local_start)) {
        AlignmentResult result;
        result.operation = internal::invalidArgument(
            "incremental alignment start overflowed timestamp range");
        return result;
    }
    Timestamp local_end = window_data.window_end_time;
    if (local_last + 1 < bucket_count) {
        if (!addNonNegativeTimestampOffset(
                window_data.window_start_time,
                static_cast<std::uint64_t>(local_last + 1) * interval_u64,
                &local_end)) {
            AlignmentResult result;
            result.operation = internal::invalidArgument(
                "incremental alignment end overflowed timestamp range");
            return result;
        }
    }

    Timestamp target_start = window_data.window_start_time;
    Timestamp target_end = window_data.window_end_time;
    if (!addNonNegativeTimestampOffset(
            window_data.window_start_time,
            static_cast<std::uint64_t>(target_first) * interval_u64,
            &target_start)) {
        return cropFullResult(alignWindowData(project_id, window_data));
    }
    if (target_last + 1 < bucket_count &&
        !addNonNegativeTimestampOffset(
            window_data.window_start_time,
            static_cast<std::uint64_t>(target_last + 1) * interval_u64,
            &target_end)) {
        return cropFullResult(alignWindowData(project_id, window_data));
    }

    WindowData local_window;
    local_window.window_start_time = local_start;
    local_window.window_end_time = local_end;
    for (const auto& [sequence_id, source_points] : window_data.sequence_values) {
        auto& selected = local_window.sequence_values[sequence_id];
        const auto local_begin = std::lower_bound(
            source_points.begin(), source_points.end(), local_start,
            [](const RawTimeseriesPoint& point, Timestamp time) {
                return point.time < time;
            });
        const auto local_finish = std::lower_bound(
            local_begin, source_points.end(), local_end,
            [](const RawTimeseriesPoint& point, Timestamp time) {
                return point.time < time;
            });
        const auto target_begin = std::lower_bound(
            local_begin, local_finish, target_start,
            [](const RawTimeseriesPoint& point, Timestamp time) {
                return point.time < time;
            });
        const auto first_target_end = [&] {
            if (target_first + 1 >= bucket_count) {
                return window_data.window_end_time;
            }
            Timestamp value = window_data.window_end_time;
            (void)addNonNegativeTimestampOffset(
                window_data.window_start_time,
                static_cast<std::uint64_t>(target_first + 1) * interval_u64,
                &value);
            return value;
        }();
        const auto last_target_start = [&] {
            Timestamp value = window_data.window_start_time;
            (void)addNonNegativeTimestampOffset(
                window_data.window_start_time,
                static_cast<std::uint64_t>(target_last) * interval_u64,
                &value);
            return value;
        }();
        const auto first_target_finish = std::lower_bound(
            target_begin, local_finish, first_target_end,
            [](const RawTimeseriesPoint& point, Timestamp time) {
                return point.time < time;
            });
        const auto last_target_begin = std::lower_bound(
            local_begin, local_finish, last_target_start,
            [](const RawTimeseriesPoint& point, Timestamp time) {
                return point.time < time;
            });
        const auto right_context_begin = std::lower_bound(
            local_begin, local_finish, target_end,
            [](const RawTimeseriesPoint& point, Timestamp time) {
                return point.time < time;
            });
        const bool has_left_context = target_begin != local_begin;
        const bool has_right_context = right_context_begin != local_finish;
        const bool has_first_target_value =
            target_begin != first_target_finish;
        const bool has_last_target_value =
            last_target_begin != local_finish &&
            last_target_begin->time < target_end;

        selected.assign(local_begin, local_finish);
        // If a boundary bucket is empty and the local slice has no usable
        // fill context, the value may depend on a much older/newer point.
        // Fall back to the full algorithm in that sparse case; this keeps the
        // incremental fast path exact for irregular data as well.
        if ((target_first != 0 && !has_first_target_value &&
             !has_left_context) ||
            (target_last + 1 < bucket_count && !has_last_target_value &&
             !has_right_context)) {
            return cropFullResult(alignWindowData(project_id, window_data));
        }
    }

    auto result = alignWindowData(project_id, local_window, config, {});
    if (result.operation.code != OperationCode::Ok &&
        result.operation.code != OperationCode::PartialSuccess) {
        return result;
    }
    auto& samples = result.aligned_data.samples;
    if (samples.empty()) {
        return result;
    }

    const auto target_offset = target_first - local_first;
    const auto target_size = target_last - target_first + 1;
    if (target_offset >= samples.size()) {
        samples.clear();
    } else {
        const auto finish_offset = std::min(
            samples.size(), target_offset + target_size);
        std::vector<AlignedSample> cropped(
            std::make_move_iterator(samples.begin() +
                                    static_cast<std::ptrdiff_t>(target_offset)),
            std::make_move_iterator(samples.begin() +
                                    static_cast<std::ptrdiff_t>(finish_offset)));
        samples = std::move(cropped);
    }
    if (samples.empty()) {
        result.aligned_data.window_start_time = range.start_time;
        result.aligned_data.window_end_time = range.end_time;
    } else {
        result.aligned_data.window_start_time = samples.front().time;
        result.aligned_data.window_end_time = samples.back().time;
    }
    result.operation = internal::ok(
        samples.size(), "affected window data aligned");
    return result;
}

AlignmentResult AlignmentService::alignWindowData(
    const WindowData& window_data,
    const AlignmentRange& range) const {
    return alignWindowData(
        window_data.project_id.empty() ? ProjectId{"default"} :
            window_data.project_id,
        window_data,
        range);
}

AlignmentResult AlignmentService::alignWindowData(
    const ProjectId& project_id,
    const WindowData& window_data,
    const AlignmentConfig& requested_config,
    const std::vector<RuntimeRelationConfig>& relations) const {
    AlignmentResult result;
    result.project_id = project_id;
    result.aligned_data.project_id = project_id;
    if (window_data.window_start_time > window_data.window_end_time) {
        result.operation = internal::invalidArgument(
            "alignment window start time must not be after end time");
        return result;
    }

    AlignmentConfig config = requested_config;
    if (config.sequences.empty()) {
        config.sequences.reserve(window_data.sequence_values.size());
        for (const auto& [sequence_id, points] : window_data.sequence_values) {
            (void)points;
            config.sequences.push_back({
                sequence_id, VariableRole::Independent,
                std::nullopt, std::nullopt});
        }
    }
    if (config.sequences.empty()) {
        result.operation = internal::invalidArgument(
            "alignment sequences must not be empty");
        return result;
    }

    std::unordered_set<SequenceId> configured_sequences;
    for (auto& sequence : config.sequences) {
        if (sequence.sequence_id.empty()) {
            result.operation = internal::invalidArgument(
                "alignment sequence_id must not be empty");
            return result;
        }
        if (!configured_sequences.emplace(sequence.sequence_id).second) {
            result.operation = internal::invalidArgument(
                "duplicate alignment sequence_id: " + sequence.sequence_id);
            return result;
        }

        SeriesKind kind = SeriesKind::Unspecified;
        if (const auto instance = configs_.findInstance(
                project_id, sequence.sequence_id)) {
            kind = instance->series_kind;
        }
        if (!sequence.aggregation) {
            sequence.aggregation = defaultAggregation(kind);
        }
        if (!sequence.fill_method) {
            sequence.fill_method = defaultFillMethod(kind);
        }
    }

    std::unordered_map<SequenceId, std::int64_t> shifts;
    std::unordered_set<std::string> relation_ids;
    std::unordered_set<SequenceId> relation_targets;
    for (const auto& relation : relations) {
        if (!relation.enabled) {
            result.operation = internal::makeOperationResult(
                OperationCode::FailedPrecondition,
                0,
                0,
                "alignment relation is not enabled: " + relation.relation_id);
            return result;
        }
        if (relation.relation_id.empty() ||
            relation.target_sequence_id.empty() ||
            relation.sources.empty()) {
            result.operation = internal::invalidArgument(
                "alignment relation must contain an id, target sequence and sources");
            return result;
        }
        if (!relation_ids.emplace(relation.relation_id).second) {
            result.operation = internal::invalidArgument(
                "duplicate alignment relation_id: " + relation.relation_id);
            return result;
        }
        if (!relation_targets.emplace(relation.target_sequence_id).second) {
            result.operation = internal::invalidArgument(
                "duplicate alignment relation target sequence: " +
                relation.target_sequence_id);
            return result;
        }
        if (configured_sequences.find(relation.target_sequence_id) ==
            configured_sequences.end()) {
            result.operation = internal::invalidArgument(
                "relation target sequence is not in alignment config: " +
                relation.target_sequence_id);
            return result;
        }
        const auto target_inserted = shifts.emplace(
            relation.target_sequence_id, 0);
        if (!target_inserted.second && target_inserted.first->second != 0) {
            result.operation = internal::invalidArgument(
                "relation target sequence has a conflicting lag: " +
                relation.target_sequence_id);
            return result;
        }

        std::unordered_set<SequenceId> relation_sources;
        for (const auto& source : relation.sources) {
            if (source.source_sequence_id.empty() ||
                source.source_sequence_id == relation.target_sequence_id) {
                result.operation = internal::invalidArgument(
                    "relation source sequence is invalid: " +
                    source.source_sequence_id);
                return result;
            }
            if (configured_sequences.find(source.source_sequence_id) ==
                configured_sequences.end()) {
                result.operation = internal::invalidArgument(
                    "relation source sequence is not in alignment config: " +
                    source.source_sequence_id);
                return result;
            }
            if (!relation_sources.emplace(source.source_sequence_id).second) {
                result.operation = internal::invalidArgument(
                    "relation contains duplicate source sequence: " +
                    source.source_sequence_id);
                return result;
            }
            const auto* fixed_lag = std::get_if<std::int64_t>(&source.lag);
            if (fixed_lag == nullptr) {
                result.operation = internal::makeOperationResult(
                    OperationCode::NotImplemented,
                    0,
                    0,
                    "alignment currently supports fixed lag only");
                return result;
            }
            const auto inserted = shifts.emplace(
                source.source_sequence_id, *fixed_lag);
            if (!inserted.second && inserted.first->second != *fixed_lag) {
                result.operation = internal::invalidArgument(
                    "source sequence has conflicting relation lags: " +
                    source.source_sequence_id);
                return result;
            }
        }
    }

    if (config.bucket_interval) {
        if (*config.bucket_interval <= 0) {
            result.operation = internal::invalidArgument(
                "alignment bucket_interval must be positive");
            return result;
        }
    } else {
        const auto inferred = inferBucketInterval(
            window_data, config.sequences);
        if (!inferred) {
            result.operation = internal::invalidArgument(
                "cannot infer bucket_interval; provide a positive interval "
                "or at least two distinct timestamps");
            return result;
        }
        config.bucket_interval = *inferred;
    }

    const auto span = static_cast<std::uint64_t>(
        window_data.window_end_time) -
        static_cast<std::uint64_t>(window_data.window_start_time);
    const auto interval = static_cast<std::uint64_t>(*config.bucket_interval);
    const auto bucket_count_u64 = span / interval + (span % interval != 0);
    if (bucket_count_u64 > std::numeric_limits<std::size_t>::max()) {
        result.operation = internal::invalidArgument(
            "alignment window contains too many buckets");
        return result;
    }
    const auto bucket_count = static_cast<std::size_t>(bucket_count_u64);

    result.aligned_data.window_start_time = window_data.window_start_time;
    result.aligned_data.window_end_time = window_data.window_end_time;
    result.aligned_data.samples.resize(bucket_count);

    const auto alignSequence = [&](std::size_t sequence_index) {
        SequenceAlignmentResult sequence_result;
        const auto& sequence_config = config.sequences[sequence_index];
        const auto source = window_data.sequence_values.find(
            sequence_config.sequence_id);
        const auto shift_it = shifts.find(sequence_config.sequence_id);
        const auto shift = shift_it == shifts.end() ? 0 : shift_it->second;

        std::string error;
        std::vector<BucketAccumulator> buckets(bucket_count);
        if (source != window_data.sequence_values.end()) {
            for (const auto& point : source->second) {
                if (point.sequence_id != sequence_config.sequence_id) {
                    sequence_result.success = false;
                    sequence_result.error =
                        "window point sequence_id does not match its map key";
                    return sequence_result;
                }
                Timestamp effective_time = 0;
                if (!addTimestamp(point.time, shift, &effective_time)) {
                    sequence_result.success = false;
                    sequence_result.error =
                        "relation lag overflowed timestamp range";
                    return sequence_result;
                }
                if (effective_time < window_data.window_start_time ||
                    effective_time >= window_data.window_end_time) {
                    continue;
                }
                const auto offset = static_cast<std::uint64_t>(effective_time) -
                    static_cast<std::uint64_t>(window_data.window_start_time);
                const auto bucket_index = static_cast<std::size_t>(
                    offset / interval);
                if (bucket_index >= bucket_count) {
                    continue;
                }
                if (!buckets[bucket_index].add(
                        point, *sequence_config.aggregation, &error)) {
                    sequence_result.success = false;
                    sequence_result.error = error;
                    return sequence_result;
                }
            }
        }

        sequence_result.values.resize(bucket_count);
        for (std::size_t bucket = 0; bucket < bucket_count; ++bucket) {
            if (!buckets[bucket].has_value) {
                continue;
            }
            TimeseriesValue value;
            if (!buckets[bucket].finish(
                    *sequence_config.aggregation, &value, &error)) {
                sequence_result.success = false;
                sequence_result.error = error;
                return sequence_result;
            }
            sequence_result.values[bucket] = std::move(value);
        }

        std::string fill_error;
        if (!fillValues(
                &sequence_result.values,
                *sequence_config.fill_method,
                &fill_error)) {
            sequence_result.success = false;
            sequence_result.error = fill_error;
            return sequence_result;
        }

        for (const auto& value : sequence_result.values) {
            if (!value) {
                ++sequence_result.missing_values;
            }
        }
        return sequence_result;
    };

    std::vector<SequenceAlignmentResult> sequence_results(
        config.sequences.size());
    constexpr std::size_t kParallelAlignmentThreshold = 16;
    constexpr std::size_t kMaxParallelAlignmentWorkers = 8;
    if (config.sequences.size() < kParallelAlignmentThreshold) {
        for (std::size_t index = 0; index < config.sequences.size(); ++index) {
            sequence_results[index] = alignSequence(index);
        }
    } else {
        const auto hardware_threads = std::thread::hardware_concurrency();
        const auto available_workers = hardware_threads == 0
            ? std::size_t{2}
            : std::min<std::size_t>(
                  kMaxParallelAlignmentWorkers, hardware_threads);
        const auto worker_count = std::min<std::size_t>(
            config.sequences.size(),
            std::max<std::size_t>(2, available_workers));
        std::vector<std::future<void>> workers;
        workers.reserve(worker_count);
        for (std::size_t worker = 0; worker < worker_count; ++worker) {
            workers.push_back(std::async(
                std::launch::async,
                [&, worker] {
                    for (std::size_t index = worker;
                         index < config.sequences.size();
                         index += worker_count) {
                        sequence_results[index] = alignSequence(index);
                    }
                }));
        }
        for (auto& worker : workers) {
            worker.get();
        }
    }

    std::size_t missing_values = 0;
    for (std::size_t sequence_index = 0;
         sequence_index < config.sequences.size();
         ++sequence_index) {
        const auto& sequence_result = sequence_results[sequence_index];
        if (!sequence_result.success) {
            result.operation = internal::invalidArgument(
                sequence_result.error);
            return result;
        }
        const auto& sequence_config = config.sequences[sequence_index];
        for (std::size_t bucket = 0; bucket < bucket_count; ++bucket) {
            if (sequence_result.values[bucket]) {
                result.aligned_data.samples[bucket].values.emplace(
                    sequence_config.sequence_id,
                    *sequence_result.values[bucket]);
            }
        }
        missing_values += sequence_result.missing_values;
    }

    for (std::size_t bucket = 0; bucket < bucket_count; ++bucket) {
        const auto bucket_offset = static_cast<std::uint64_t>(bucket) * interval;
        Timestamp time = 0;
        if (!addNonNegativeTimestampOffset(
                window_data.window_start_time, bucket_offset, &time)) {
            result.operation = internal::invalidArgument(
                "alignment bucket time overflowed timestamp range");
            return result;
        }
        result.aligned_data.samples[bucket].time = time;
    }

    if (bucket_count != 0 &&
        config.sequences.size() >
            std::numeric_limits<std::size_t>::max() / bucket_count) {
        result.operation = internal::invalidArgument(
            "alignment result contains too many values");
        return result;
    }
    const auto total_values = bucket_count * config.sequences.size();
    if (missing_values != 0) {
        result.operation = internal::makeOperationResult(
            OperationCode::PartialSuccess,
            total_values - missing_values,
            missing_values,
            "alignment completed with missing values after fill");
    } else {
        result.operation = internal::ok(
            total_values, "window data aligned");
    }
    return result;
}

AlignmentResult AlignmentService::alignWindowData(
    const WindowData& window_data,
    const AlignmentConfig& config,
    const std::vector<RuntimeRelationConfig>& relations) const {
    return alignWindowData(
        window_data.project_id.empty() ? ProjectId{"default"} :
            window_data.project_id,
        window_data,
        config,
        relations);
}

}  // namespace sfkg::timeseries::core
