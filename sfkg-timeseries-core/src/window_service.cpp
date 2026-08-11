#include "sfkg/timeseries/core/window_service.hpp"

#include <algorithm>
#include <limits>
#include <string>
#include <unordered_set>

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core {

std::int64_t WindowService::windowSize() const {
    std::lock_guard lock(mutex_);
    return window_size_;
}

OperationResult WindowService::configureWindowSize(std::int64_t window_size) {
    if (window_size <= 0) {
        return internal::invalidArgument(
            "window_size must be positive");
    }

    std::lock_guard lock(mutex_);
    window_size_ = window_size;
    pruneExpiredPoints();
    return internal::ok(0, "hot window size configured");
}

OperationResult WindowService::buildTimeWindow(
    const TimeseriesBatch& data) {
    return updateWindowIncremental(data, std::nullopt).operation;
}

OperationResult WindowService::buildTimeWindow(
    const TimeseriesBatch& data,
    std::int64_t window_size) {
    return updateWindowIncremental(data, window_size).operation;
}

WindowUpdateResult WindowService::buildTimeWindowIncremental(
    const TimeseriesBatch& data) {
    return updateWindowIncremental(data, std::nullopt);
}

OperationResult WindowService::replaceDerivedSequence(
    const SequenceId& sequence_id,
    const TimeseriesBatch& data) {
    if (sequence_id.empty()) {
        return internal::invalidArgument(
            "derived sequence_id must not be empty");
    }
    for (const auto& point : data.points) {
        if (point.sequence_id != sequence_id) {
            return internal::invalidArgument(
                "derived point sequence_id does not match output sequence");
        }
    }

    std::lock_guard lock(mutex_);
    sequence_windows_.erase(sequence_id);
    auto& output = sequence_windows_[sequence_id];
    for (const auto& point : data.points) {
        output[point.time] = point;
    }
    if (output.empty()) {
        sequence_windows_.erase(sequence_id);
    }
    pruneExpiredPoints();
    return internal::ok(data.points.size(), "derived hot window replaced");
}

OperationResult WindowService::patchDerivedSequence(
    const SequenceId& sequence_id,
    Timestamp start_time,
    Timestamp end_time,
    const TimeseriesBatch& data) {
    if (sequence_id.empty()) {
        return internal::invalidArgument(
            "derived sequence_id must not be empty");
    }
    if (start_time > end_time) {
        return internal::invalidArgument(
            "derived patch start time must not be after end time");
    }
    for (const auto& point : data.points) {
        if (point.sequence_id != sequence_id) {
            return internal::invalidArgument(
                "derived point sequence_id does not match output sequence");
        }
        if (point.time < start_time || point.time > end_time) {
            return internal::invalidArgument(
                "derived patch contains a point outside its affected range");
        }
    }

    std::lock_guard lock(mutex_);
    auto sequence = sequence_windows_.find(sequence_id);
    if (sequence != sequence_windows_.end()) {
        auto& points = sequence->second;
        auto begin = points.lower_bound(start_time);
        auto finish = end_time == std::numeric_limits<Timestamp>::max()
            ? points.end()
            : points.upper_bound(end_time);
        points.erase(begin, finish);
        if (points.empty()) {
            sequence_windows_.erase(sequence);
        }
    }
    auto& output = sequence_windows_[sequence_id];
    for (const auto& point : data.points) {
        output[point.time] = point;
    }
    if (output.empty()) {
        sequence_windows_.erase(sequence_id);
    }
    pruneExpiredPoints();
    return internal::ok(data.points.size(), "derived hot window patched");
}

OperationResult WindowService::updateWindow(
    const TimeseriesBatch& data,
    std::optional<std::int64_t> window_size_override) {
    return updateWindowIncremental(data, window_size_override).operation;
}

WindowUpdateResult WindowService::updateWindowIncremental(
    const TimeseriesBatch& data,
    std::optional<std::int64_t> window_size_override) {
    WindowUpdateResult result;
    if (data.points.empty()) {
        result.operation = internal::invalidArgument(
            "window input must not be empty");
        return result;
    }
    if (window_size_override && *window_size_override <= 0) {
        result.operation = internal::invalidArgument(
            "window_size must be positive");
        return result;
    }
    for (const auto& point : data.points) {
        if (point.sequence_id.empty()) {
            result.operation = internal::invalidArgument(
                "window point sequence_id must not be empty");
            return result;
        }
    }

    result.incremental_safe = true;
    std::lock_guard lock(mutex_);
    const auto old_watermark = watermark_;
    const auto old_window_size = window_size_;
    if (window_size_override) {
        window_size_ = *window_size_override;
    }

    std::unordered_set<SequenceId> seen_sequence_ids;
    result.changed_sequence_ids.reserve(data.points.size());
    for (const auto& point : data.points) {
        if (seen_sequence_ids.insert(point.sequence_id).second) {
            result.changed_sequence_ids.push_back(point.sequence_id);
        }
        if (!result.affected_start_time ||
            point.time < *result.affected_start_time) {
            result.affected_start_time = point.time;
        }
        if (!result.affected_end_time ||
            point.time > *result.affected_end_time) {
            result.affected_end_time = point.time;
        }

        auto& sequence = sequence_windows_[point.sequence_id];
        if (sequence.find(point.time) != sequence.end() ||
            (!sequence.empty() && point.time < sequence.rbegin()->first)) {
            // An out-of-order insert can change sample-offset anchors and
            // interpolation results across a wider portion of the window.
            // Replacing an existing timestamp is treated the same way: its
            // new value may change later interpolation results.
            result.incremental_safe = false;
        }
        sequence[point.time] = point;
        if (!watermark_ || point.time > *watermark_) {
            watermark_ = point.time;
        }
    }

    if (old_watermark && watermark_) {
        const auto old_start = *old_watermark <
                std::numeric_limits<Timestamp>::min() + old_window_size
            ? std::numeric_limits<Timestamp>::min()
            : *old_watermark - old_window_size;
        const auto new_start = *watermark_ <
                std::numeric_limits<Timestamp>::min() + window_size_
            ? std::numeric_limits<Timestamp>::min()
            : *watermark_ - window_size_;
        bool will_evict = false;
        if (new_start > old_start || window_size_ != old_window_size) {
            for (const auto& [sequence_id, sequence] : sequence_windows_) {
                (void)sequence_id;
                if (!sequence.empty() && sequence.begin()->first < new_start) {
                    will_evict = true;
                    break;
                }
            }
        }
        if (will_evict) {
            // We intentionally fall back to a full refresh when the moving
            // boundary advances; incremental consumers must remove expired
            // derived values and constraint state as well.
            result.incremental_safe = false;
        }
    }

    pruneExpiredPoints();
    if (!result.incremental_safe &&
        !result.affected_start_time.has_value()) {
        result.affected_start_time = std::nullopt;
        result.affected_end_time = std::nullopt;
    }
    result.operation = internal::ok(
        data.points.size(), "hot window updated");
    return result;
}

void WindowService::pruneExpiredPoints() {
    if (!watermark_ || window_size_ <= 0) {
        return;
    }
    const auto start = *watermark_ <
            std::numeric_limits<Timestamp>::min() + window_size_
        ? std::numeric_limits<Timestamp>::min()
        : *watermark_ - window_size_;
    for (auto sequence = sequence_windows_.begin();
         sequence != sequence_windows_.end();) {
        auto& points = sequence->second;
        points.erase(points.begin(), points.lower_bound(start));
        if (points.empty()) {
            sequence = sequence_windows_.erase(sequence);
        } else {
            ++sequence;
        }
    }
}

WindowQueryResult WindowService::queryWindowData(
    const WindowQuery& query) const {
    WindowQueryResult result;
    if (query.sequence_ids.empty()) {
        result.operation = internal::invalidArgument(
            "window query sequence_ids must not be empty");
        return result;
    }
    if (query.start_time && query.end_time &&
        *query.start_time > *query.end_time) {
        result.operation = internal::invalidArgument(
            "window query start_time must not be after end_time");
        return result;
    }

    std::lock_guard lock(mutex_);
    if (!watermark_ || window_size_ <= 0) {
        result.operation = internal::ok(0, "window is empty");
        return result;
    }

    const auto default_end = *watermark_ ==
            std::numeric_limits<Timestamp>::max()
        ? *watermark_
        : *watermark_ + 1;
    const auto end = query.end_time.value_or(default_end);
    const auto default_start = end <
            std::numeric_limits<Timestamp>::min() + window_size_
        ? std::numeric_limits<Timestamp>::min()
        : end - window_size_;
    const auto start = query.start_time.value_or(default_start);
    if (start > end) {
        result.operation = internal::invalidArgument(
            "window query time range is invalid");
        return result;
    }

    std::size_t count = 0;
    for (const auto& sequence_id : query.sequence_ids) {
        const auto sequence = sequence_windows_.find(sequence_id);
        if (sequence == sequence_windows_.end()) {
            continue;
        }
        auto& output = result.data.sequence_values[sequence_id];
        const auto begin = sequence->second.lower_bound(start);
        const auto finish = sequence->second.lower_bound(end);
        for (auto point = begin; point != finish; ++point) {
            output.push_back(point->second);
            ++count;
        }
        if (output.empty()) {
            result.data.sequence_values.erase(sequence_id);
        }
    }
    result.data.window_start_time = start;
    result.data.window_end_time = end;
    result.operation = internal::ok(count, "window query completed");
    return result;
}

}  // namespace sfkg::timeseries::core
