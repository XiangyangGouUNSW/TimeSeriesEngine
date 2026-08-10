#include "sfkg/timeseries/core/window_service.hpp"

#include <algorithm>
#include <limits>
#include <string>

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
    return updateWindow(data, std::nullopt);
}

OperationResult WindowService::buildTimeWindow(
    const TimeseriesBatch& data,
    std::int64_t window_size) {
    return updateWindow(data, window_size);
}

OperationResult WindowService::updateWindow(
    const TimeseriesBatch& data,
    std::optional<std::int64_t> window_size_override) {
    if (data.points.empty()) {
        return internal::invalidArgument("window input must not be empty");
    }
    if (window_size_override && *window_size_override <= 0) {
        return internal::invalidArgument(
            "window_size must be positive");
    }
    for (const auto& point : data.points) {
        if (point.sequence_id.empty()) {
            return internal::invalidArgument(
                "window point sequence_id must not be empty");
        }
    }

    std::lock_guard lock(mutex_);
    if (window_size_override) {
        window_size_ = *window_size_override;
    }
    for (const auto& point : data.points) {
        sequence_windows_[point.sequence_id][point.time] = point;
        if (!watermark_ || point.time > *watermark_) {
            watermark_ = point.time;
        }
    }

    pruneExpiredPoints();
    return internal::ok(data.points.size(), "hot window updated");
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
