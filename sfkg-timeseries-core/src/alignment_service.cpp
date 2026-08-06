#include "sfkg/timeseries/core/alignment_service.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core {
namespace {

using BucketPoint = std::pair<Timestamp, const RawTimeseriesPoint*>;
using BucketPoints = std::vector<std::vector<BucketPoint>>;
using BucketValues = std::vector<std::optional<TimeseriesValue>>;

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

bool aggregateBucket(
    const std::vector<BucketPoint>& points,
    BucketAggregation aggregation,
    TimeseriesValue* output,
    std::string* error) {
    if (points.empty()) {
        return false;
    }

    if (aggregation == BucketAggregation::First ||
        aggregation == BucketAggregation::Last) {
        const auto comparator = [](const BucketPoint& left,
                                   const BucketPoint& right) {
            return left.first < right.first;
        };
        const auto selected = aggregation == BucketAggregation::First
            ? std::min_element(points.begin(), points.end(), comparator)
            : std::max_element(points.begin(), points.end(), comparator);
        *output = selected->second->value;
        return true;
    }

    double accumulated = 0.0;
    double selected = 0.0;
    for (std::size_t index = 0; index < points.size(); ++index) {
        double value = 0.0;
        if (!numericValue(points[index].second->value, &value)) {
            *error =
                "numeric aggregation requires finite numeric sequence values";
            return false;
        }
        if (aggregation == BucketAggregation::Average) {
            accumulated += value;
        } else if (index == 0 ||
                   (aggregation == BucketAggregation::Maximum &&
                    value > selected) ||
                   (aggregation == BucketAggregation::Minimum &&
                    value < selected)) {
            selected = value;
        }
    }

    if (aggregation == BucketAggregation::Average) {
        accumulated /= static_cast<double>(points.size());
        if (!std::isfinite(accumulated)) {
            *error = "average aggregation produced a non-finite value";
            return false;
        }
        *output = accumulated;
    } else {
        *output = selected;
    }
    return true;
}

std::optional<std::size_t> previousValue(
    const BucketValues& values,
    std::size_t index) {
    for (std::size_t current = index; current > 0; --current) {
        if (values[current - 1]) {
            return current - 1;
        }
    }
    return std::nullopt;
}

std::optional<std::size_t> nextValue(
    const BucketValues& values,
    std::size_t index) {
    for (std::size_t current = index + 1; current < values.size(); ++current) {
        if (values[current]) {
            return current;
        }
    }
    return std::nullopt;
}

void fillNearAt(
    const BucketValues& source,
    BucketValues* values,
    std::size_t index) {
    const auto previous = previousValue(source, index);
    const auto next = nextValue(source, index);
    if (!previous && !next) {
        return;
    }
    if (!previous) {
        (*values)[index] = source[*next];
        return;
    }
    if (!next) {
        (*values)[index] = source[*previous];
        return;
    }

    const auto previous_distance = index - *previous;
    const auto next_distance = *next - index;
    (*values)[index] = previous_distance <= next_distance
        ? source[*previous]
        : source[*next];
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

    for (std::size_t index = 0;
         index < static_cast<std::size_t>(first - values->begin());
         ++index) {
        fillNearAt(original, values, index);
    }
    for (std::size_t index = static_cast<std::size_t>(last - values->begin()) + 1;
         index < values->size();
         ++index) {
        fillNearAt(original, values, index);
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
            for (std::size_t index = 0; index < values->size(); ++index) {
                if (!(*values)[index]) {
                    fillNearAt(original, values, index);
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
    const WindowData& window_data,
    const AlignmentConfig& config,
    const std::vector<RuntimeRelationConfig>& relations) const {
    AlignmentResult result;
    if (window_data.window_start_time > window_data.window_end_time) {
        result.operation = internal::invalidArgument(
            "alignment window start time must not be after end time");
        return result;
    }
    if (config.bucket_interval <= 0) {
        result.operation = internal::invalidArgument(
            "alignment bucket_interval must be positive");
        return result;
    }
    if (config.sequences.empty()) {
        result.operation = internal::invalidArgument(
            "alignment sequences must not be empty");
        return result;
    }

    std::unordered_set<SequenceId> configured_sequences;
    for (const auto& sequence : config.sequences) {
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

    const auto span = static_cast<std::uint64_t>(
        window_data.window_end_time) -
        static_cast<std::uint64_t>(window_data.window_start_time);
    const auto interval = static_cast<std::uint64_t>(config.bucket_interval);
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

    std::size_t missing_values = 0;
    for (std::size_t sequence_index = 0;
         sequence_index < config.sequences.size();
         ++sequence_index) {
        const auto& sequence_config = config.sequences[sequence_index];
        const auto source = window_data.sequence_values.find(
            sequence_config.sequence_id);
        const auto shift_it = shifts.find(sequence_config.sequence_id);
        const auto shift = shift_it == shifts.end() ? 0 : shift_it->second;

        BucketPoints buckets(bucket_count);
        if (source != window_data.sequence_values.end()) {
            for (const auto& point : source->second) {
                if (point.sequence_id != sequence_config.sequence_id) {
                    result.operation = internal::invalidArgument(
                        "window point sequence_id does not match its map key");
                    return result;
                }
                Timestamp effective_time = 0;
                if (!addTimestamp(point.time, shift, &effective_time)) {
                    result.operation = internal::invalidArgument(
                        "relation lag overflowed timestamp range");
                    return result;
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
                buckets[bucket_index].push_back({effective_time, &point});
            }
        }

        BucketValues values(bucket_count);
        for (std::size_t bucket = 0; bucket < bucket_count; ++bucket) {
            if (buckets[bucket].empty()) {
                continue;
            }
            TimeseriesValue value;
            std::string error;
            if (!aggregateBucket(
                    buckets[bucket], sequence_config.aggregation,
                    &value, &error)) {
                result.operation = internal::invalidArgument(error);
                return result;
            }
            values[bucket] = std::move(value);
        }

        std::string fill_error;
        if (!fillValues(
                &values, sequence_config.fill_method, &fill_error)) {
            result.operation = internal::invalidArgument(fill_error);
            return result;
        }

        for (std::size_t bucket = 0; bucket < bucket_count; ++bucket) {
            if (values[bucket]) {
                result.aligned_data.samples[bucket].values.emplace(
                    sequence_config.sequence_id, *values[bucket]);
            } else {
                ++missing_values;
            }
        }
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

}  // namespace sfkg::timeseries::core
