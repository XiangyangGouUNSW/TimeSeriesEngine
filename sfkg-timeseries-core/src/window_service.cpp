#include "sfkg/timeseries/core/window_service.hpp"

#include <algorithm>
#include <chrono>
#include <future>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_set>
#include <utility>

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core {

void WindowService::replaceSequence(
    SequenceWindow& sequence,
    const TimeseriesBatch& data) {
    sequence.points = data.points;
    std::stable_sort(
        sequence.points.begin(),
        sequence.points.end(),
        [](const RawTimeseriesPoint& left, const RawTimeseriesPoint& right) {
            return left.time < right.time;
        });

    std::vector<RawTimeseriesPoint> unique_points;
    unique_points.reserve(sequence.points.size());
    for (const auto& point : sequence.points) {
        if (!unique_points.empty() &&
            unique_points.back().time == point.time) {
            // stable_sort preserves input order, so the last duplicate wins,
            // matching the old map assignment behavior.
            unique_points.back() = point;
        } else {
            unique_points.push_back(point);
        }
    }
    sequence.points = std::move(unique_points);
    sequence.active_begin = 0;
    sequence.late_points.clear();
    sequence.latest_time = sequence.points.empty()
        ? std::nullopt
        : std::optional<Timestamp>{sequence.points.back().time};
}

void WindowService::mergeSortedPoints(
    SequenceWindow& sequence,
    std::vector<RawTimeseriesPoint> incoming) {
    compactSequence(sequence);
    if (incoming.empty()) {
        return;
    }

    // Keep enough spare capacity for the whole correction batch. If the
    // vector already has room, the common append case changes only the new
    // suffix and does not copy any existing point.
    const auto required_capacity = sequence.points.size() + incoming.size();
    if (sequence.points.capacity() < required_capacity) {
        sequence.points.reserve(required_capacity);
    }
    if (sequence.points.empty() ||
        incoming.front().time > sequence.points.back().time) {
        for (auto& point : incoming) {
            sequence.points.push_back(std::move(point));
        }
    } else {
        // Incoming points are sorted. Process them in order and keep a
        // monotonic insertion index. A duplicate replaces one element; a new
        // timestamp shifts only the vector suffix after its insertion point,
        // instead of rebuilding and copying the entire vector.
        std::size_t search_begin = 0;
        for (auto& point : incoming) {
            const auto begin = sequence.points.begin() +
                static_cast<std::ptrdiff_t>(search_begin);
            const auto position = std::lower_bound(
                begin,
                sequence.points.end(),
                point.time,
                [](const RawTimeseriesPoint& existing, Timestamp time) {
                    return existing.time < time;
                });
            const auto index = static_cast<std::size_t>(
                position - sequence.points.begin());
            if (position != sequence.points.end() &&
                position->time == point.time) {
                *position = std::move(point);
                search_begin = index + 1;
            } else {
                sequence.points.insert(position, std::move(point));
                search_begin = index + 1;
            }
        }
    }
    sequence.active_begin = 0;
    if (!sequence.points.empty()) {
        sequence.latest_time = sequence.points.back().time;
    } else if (sequence.late_points.empty()) {
        sequence.latest_time = std::nullopt;
    }
}

void WindowService::flushLatePoints(SequenceWindow& sequence) {
    if (sequence.late_points.empty()) {
        return;
    }
    std::vector<RawTimeseriesPoint> incoming;
    incoming.reserve(sequence.late_points.size());
    for (auto& [time, point] : sequence.late_points) {
        (void)time;
        incoming.push_back(std::move(point));
    }
    sequence.late_points.clear();
    mergeSortedPoints(sequence, std::move(incoming));
}

void WindowService::compactSequence(SequenceWindow& sequence) {
    if (sequence.active_begin == 0) {
        return;
    }
    if (sequence.active_begin >= sequence.points.size()) {
        sequence.points.clear();
        sequence.active_begin = 0;
        return;
    }

    // Physical compaction is deliberately amortized. Normal eviction only
    // advances active_begin and therefore does not move every live point.
    sequence.points.erase(
        sequence.points.begin(),
        sequence.points.begin() +
            static_cast<std::ptrdiff_t>(sequence.active_begin));
    sequence.active_begin = 0;
}

std::int64_t WindowService::windowSize() const {
    std::shared_lock lock(mutex_);
    return window_size_;
}

std::shared_ptr<WindowService::SequenceWindow>
WindowService::sequenceWindowFor(const SequenceId& sequence_id) {
    std::unique_lock lock(mutex_);
    auto& sequence = sequence_windows_[sequence_id];
    if (!sequence) {
        sequence = std::make_shared<SequenceWindow>();
    }
    return sequence;
}

OperationResult WindowService::configureWindowSize(std::int64_t window_size) {
    if (window_size <= 0) {
        return internal::invalidArgument(
            "window_size must be positive");
    }

    std::vector<std::shared_ptr<SequenceWindow>> sequences;
    Timestamp window_start = 0;
    bool has_watermark = false;
    {
        std::unique_lock lock(mutex_);
        window_size_ = window_size;
        has_watermark = watermark_.has_value();
        if (has_watermark) {
            window_start = *watermark_ <
                    std::numeric_limits<Timestamp>::min() + window_size_
                ? std::numeric_limits<Timestamp>::min()
                : *watermark_ - window_size_;
        }
        sequences.reserve(sequence_windows_.size());
        for (const auto& [sequence_id, sequence] : sequence_windows_) {
            (void)sequence_id;
            sequences.push_back(sequence);
        }
    }
    if (!sequences.empty() && has_watermark) {
        (void)pruneExpiredPoints(window_start, sequences);
    }
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

    const auto output = sequenceWindowFor(sequence_id);
    {
        std::unique_lock lock(output->mutex);
        replaceSequence(*output, data);
    }
    std::vector<std::shared_ptr<SequenceWindow>> sequences;
    Timestamp window_start = 0;
    bool has_watermark = false;
    {
        std::shared_lock lock(mutex_);
        has_watermark = watermark_.has_value();
        if (has_watermark) {
            window_start = *watermark_ <
                    std::numeric_limits<Timestamp>::min() + window_size_
                ? std::numeric_limits<Timestamp>::min()
                : *watermark_ - window_size_;
        }
        for (const auto& [id, sequence] : sequence_windows_) {
            (void)id;
            sequences.push_back(sequence);
        }
    }
    if (has_watermark) {
        (void)pruneExpiredPoints(window_start, sequences);
    }
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

    const auto output = sequenceWindowFor(sequence_id);
    std::unique_lock lock(output->mutex);
    auto& points = *output;
    flushLatePoints(points);
    compactSequence(points);
    const auto begin = std::lower_bound(
        points.points.begin(),
        points.points.end(),
        start_time,
        [](const RawTimeseriesPoint& point, Timestamp time) {
            return point.time < time;
        });
    const auto finish = end_time == std::numeric_limits<Timestamp>::max()
        ? points.points.end()
        : std::upper_bound(
              points.points.begin(),
              points.points.end(),
              end_time,
              [](Timestamp time, const RawTimeseriesPoint& point) {
                  return time < point.time;
              });
    points.points.erase(begin, finish);
    // The patch range is normally small. Rebuilding the affected segment in
    // sorted order keeps duplicate-timestamp and arbitrary patch ordering
    // semantics explicit without requiring a second index structure.
    std::vector<RawTimeseriesPoint> patch = data.points;
    std::stable_sort(
        patch.begin(),
        patch.end(),
        [](const RawTimeseriesPoint& left, const RawTimeseriesPoint& right) {
            return left.time < right.time;
        });
    std::vector<RawTimeseriesPoint> unique_patch;
    unique_patch.reserve(patch.size());
    for (const auto& point : patch) {
        if (!unique_patch.empty() &&
            unique_patch.back().time == point.time) {
            unique_patch.back() = point;
        } else {
            unique_patch.push_back(point);
        }
    }
    mergeSortedPoints(points, std::move(unique_patch));
    lock.unlock();
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

    std::unordered_map<SequenceId, std::vector<RawTimeseriesPoint>>
        incoming_by_sequence;
    incoming_by_sequence.reserve(data.points.size());
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
        incoming_by_sequence[point.sequence_id].push_back(point);
    }

    std::vector<std::shared_ptr<SequenceWindow>> all_sequences;
    std::unordered_map<SequenceId, std::shared_ptr<SequenceWindow>>
        sequence_refs;
    Timestamp window_start = 0;
    {
        std::unique_lock lock(mutex_);
        if (window_size_override) {
            window_size_ = *window_size_override;
        }
        for (const auto& point : data.points) {
            if (!watermark_ || point.time > *watermark_) {
                watermark_ = point.time;
            }
        }
        if (update_generation_ != std::numeric_limits<std::uint64_t>::max()) {
            ++update_generation_;
        }
        result.update_generation = update_generation_;
        if (watermark_) {
            window_start = *watermark_ <
                    std::numeric_limits<Timestamp>::min() + window_size_
                ? std::numeric_limits<Timestamp>::min()
                : *watermark_ - window_size_;
            result.window_start_time = window_start;
        }
        sequence_refs.reserve(incoming_by_sequence.size());
        for (const auto& [sequence_id, points] : incoming_by_sequence) {
            (void)points;
            auto& sequence = sequence_windows_[sequence_id];
            if (!sequence) {
                sequence = std::make_shared<SequenceWindow>();
            }
            sequence_refs.emplace(sequence_id, sequence);
        }
        all_sequences.reserve(sequence_windows_.size());
        for (const auto& [sequence_id, sequence] : sequence_windows_) {
            (void)sequence_id;
            all_sequences.push_back(sequence);
        }
    }

    result.incremental_safe = true;
    struct PreparedSequenceUpdate {
        std::shared_ptr<SequenceWindow> sequence;
        std::vector<RawTimeseriesPoint> incoming;
        bool strictly_increasing{false};
    };
    struct SequenceUpdateTiming {
        bool incremental_safe{false};
        double lock_wait_ms{0.0};
        double update_ms{0.0};
    };

    std::vector<PreparedSequenceUpdate> prepared_updates;
    prepared_updates.reserve(incoming_by_sequence.size());
    for (auto& [sequence_id, incoming] : incoming_by_sequence) {
        bool strictly_increasing = true;
        for (std::size_t index = 1; index < incoming.size(); ++index) {
            if (incoming[index].time <= incoming[index - 1].time) {
                strictly_increasing = false;
                break;
            }
        }

        // Sorting and duplicate removal are pure local work. Do them before
        // taking the sequence lock so an out-of-order correction cannot hold
        // up readers or another ingest batch while allocating and sorting.
        if (!strictly_increasing) {
            std::stable_sort(
                incoming.begin(),
                incoming.end(),
                [](const RawTimeseriesPoint& left,
                   const RawTimeseriesPoint& right) {
                    return left.time < right.time;
                });
            std::vector<RawTimeseriesPoint> unique_incoming;
            unique_incoming.reserve(incoming.size());
            for (auto& point : incoming) {
                if (!unique_incoming.empty() &&
                    unique_incoming.back().time == point.time) {
                    unique_incoming.back() = std::move(point);
                } else {
                    unique_incoming.push_back(std::move(point));
                }
            }
            incoming = std::move(unique_incoming);
        }
        prepared_updates.push_back({
            sequence_refs.at(sequence_id),
            std::move(incoming),
            strictly_increasing});
    }

    std::vector<std::future<SequenceUpdateTiming>> updates;
    updates.reserve(prepared_updates.size());
    for (auto& prepared : prepared_updates) {
        updates.push_back(std::async(
            std::launch::async,
            [sequence = prepared.sequence,
             incoming = std::move(prepared.incoming),
             strictly_increasing = prepared.strictly_increasing]() mutable {
                SequenceUpdateTiming timing;
                const auto wait_started = std::chrono::steady_clock::now();
                std::unique_lock lock(sequence->mutex);
                const auto lock_acquired = std::chrono::steady_clock::now();
                timing.lock_wait_ms =
                    std::chrono::duration<double, std::milli>(
                        lock_acquired - wait_started).count();

                const auto has_active_points = !sequence->empty();
                const bool append_only = strictly_increasing &&
                    !incoming.empty() &&
                    (!sequence->latest_time ||
                     incoming.front().time > *sequence->latest_time);
                const auto update_started = lock_acquired;
                if (append_only) {
                    const auto required_capacity =
                        sequence->points.size() + incoming.size();
                    if (sequence->points.capacity() < required_capacity) {
                        const auto doubled_capacity = sequence->points.capacity() >
                                std::numeric_limits<std::size_t>::max() / 2
                            ? std::numeric_limits<std::size_t>::max()
                            : sequence->points.capacity() * 2;
                        sequence->points.reserve(std::max(
                            required_capacity,
                            std::max<std::size_t>(1, doubled_capacity)));
                    }
                    for (auto& point : incoming) {
                        sequence->points.push_back(std::move(point));
                        sequence->latest_time = sequence->points.back().time;
                    }
                    timing.incremental_safe = true;
                } else if (!has_active_points && sequence->late_points.empty()) {
                    mergeSortedPoints(*sequence, std::move(incoming));
                } else {
                    for (auto& point : incoming) {
                        const auto point_time = point.time;
                        sequence->late_points[point.time] = std::move(point);
                        if (!sequence->latest_time ||
                            point_time > *sequence->latest_time) {
                            sequence->latest_time = point_time;
                        }
                    }
                    if (sequence->late_points.size() >=
                        kLatePointFlushThreshold) {
                        // Corrections remain cheap while sparse, but the
                        // side buffer is periodically folded back into the
                        // append-oriented vector so it cannot grow without
                        // bound or turn every query into a large merge.
                        flushLatePoints(*sequence);
                    }
                }
                timing.update_ms =
                    std::chrono::duration<double, std::milli>(
                        std::chrono::steady_clock::now() - update_started).count();
                return timing;
            }));
    }
    for (auto& update : updates) {
        const auto timing = update.get();
        result.incremental_safe =
            timing.incremental_safe && result.incremental_safe;
        result.sequence_lock_wait_ms += timing.lock_wait_ms;
        result.sequence_update_ms += timing.update_ms;
    }

    // Eviction is not an ordering failure. It is removed independently while
    // the per-sequence update work above can run concurrently.
    result.window_evicted = pruneExpiredPoints(
        window_start,
        all_sequences,
        &result.eviction_lock_wait_ms,
        &result.eviction_update_ms);
    result.operation = internal::ok(
        data.points.size(), "hot window updated");
    return result;
}

bool WindowService::pruneExpiredPoints(
    Timestamp start,
    const std::vector<std::shared_ptr<SequenceWindow>>& sequences,
    double* lock_wait_ms,
    double* update_ms) {
    if (sequences.empty()) {
        return false;
    }

    // Eviction touches independent per-sequence buffers.  Keep the global
    // metadata lock out of this phase and let each sequence use its own lock;
    // this is safe because callers pass a stable snapshot of shared_ptrs.
    struct PruneTiming {
        bool evicted{false};
        double lock_wait_ms{0.0};
        double update_ms{0.0};
    };
    auto pruneOne = [start](const std::shared_ptr<SequenceWindow>& sequence) {
        PruneTiming timing;
        const auto wait_started = std::chrono::steady_clock::now();
        bool evicted = false;
        std::unique_lock lock(sequence->mutex);
        const auto lock_acquired = std::chrono::steady_clock::now();
        timing.lock_wait_ms =
            std::chrono::duration<double, std::milli>(
                lock_acquired - wait_started).count();
        const auto update_started = lock_acquired;
        auto& points = *sequence;
        const auto begin = points.points.begin() +
            static_cast<std::ptrdiff_t>(points.active_begin);
        const auto first_active = std::lower_bound(
            begin,
            points.points.end(),
            start,
            [](const RawTimeseriesPoint& point, Timestamp time) {
                return point.time < time;
            });
        if (first_active != begin) {
            evicted = true;
        }
        const auto late_begin = points.late_points.begin();
        const auto late_end = points.late_points.lower_bound(start);
        if (late_begin != late_end) {
            evicted = true;
        }
        points.active_begin = static_cast<std::size_t>(
            first_active - points.points.begin());
        points.late_points.erase(late_begin, late_end);
        // Keep physical memory bounded while avoiding a move on every ingest.
        if (points.active_begin >= 4096 &&
            points.active_begin * 2 >= points.points.size()) {
            compactSequence(points);
        }
        timing.evicted = evicted;
        timing.update_ms =
            std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - update_started).count();
        return timing;
    };

    // For the usual small ETTh1-style window, thread creation costs more than
    // the lower_bound calls.  Reserve the parallel path for a genuinely wide
    // variable set; the per-sequence locks still keep it safe there.
    constexpr std::size_t kParallelPruneThreshold = 16;
    if (sequences.size() < kParallelPruneThreshold) {
        bool evicted = false;
        for (const auto& sequence : sequences) {
            const auto timing = pruneOne(sequence);
            evicted = timing.evicted || evicted;
            if (lock_wait_ms != nullptr) {
                *lock_wait_ms += timing.lock_wait_ms;
            }
            if (update_ms != nullptr) {
                *update_ms += timing.update_ms;
            }
        }
        return evicted;
    }

    std::vector<std::future<PruneTiming>> futures;
    futures.reserve(sequences.size());
    for (const auto& sequence : sequences) {
        futures.push_back(std::async(
            std::launch::async, pruneOne, sequence));
    }
    bool evicted = false;
    for (auto& future : futures) {
        const auto timing = future.get();
        evicted = timing.evicted || evicted;
        if (lock_wait_ms != nullptr) {
            *lock_wait_ms += timing.lock_wait_ms;
        }
        if (update_ms != nullptr) {
            *update_ms += timing.update_ms;
        }
    }
    return evicted;
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

    std::vector<std::pair<SequenceId, std::shared_ptr<SequenceWindow>>>
        sequences;
    std::optional<Timestamp> watermark;
    std::int64_t window_size = 0;
    {
        std::shared_lock lock(mutex_);
        watermark = watermark_;
        window_size = window_size_;
        sequences.reserve(query.sequence_ids.size());
        for (const auto& sequence_id : query.sequence_ids) {
            const auto found = sequence_windows_.find(sequence_id);
            if (found != sequence_windows_.end() && found->second) {
                sequences.emplace_back(sequence_id, found->second);
            }
        }
    }
    if (!watermark || window_size <= 0) {
        result.operation = internal::ok(0, "window is empty");
        return result;
    }

    const auto live_end = *watermark ==
            std::numeric_limits<Timestamp>::max()
        ? *watermark
        : *watermark + 1;
    const auto live_start = live_end <
            std::numeric_limits<Timestamp>::min() + window_size
        ? std::numeric_limits<Timestamp>::min()
        : live_end - window_size;
    const auto end = query.end_time.value_or(live_end);
    const auto start = query.start_time.value_or(live_start);
    if (start > end) {
        result.operation = internal::invalidArgument(
            "window query time range is invalid");
        return result;
    }

    std::size_t count = 0;
    result.data.window_start_time = query.preserve_window_bounds
        ? live_start
        : start;
    result.data.window_end_time = query.preserve_window_bounds
        ? live_end
        : end;
    for (const auto& [sequence_id, sequence] : sequences) {
        std::shared_lock sequence_lock(sequence->mutex);
        const auto& points = *sequence;
        const auto active_begin = points.points.begin() +
            static_cast<std::ptrdiff_t>(points.active_begin);
        const auto vector_begin = std::lower_bound(
            active_begin,
            points.points.end(),
            start,
            [](const RawTimeseriesPoint& point, Timestamp time) {
                return point.time < time;
            });
        const auto vector_finish = std::lower_bound(
            vector_begin,
            points.points.end(),
            end,
            [](const RawTimeseriesPoint& point, Timestamp time) {
                return point.time < time;
            });
        auto& output = result.data.sequence_values[sequence_id];
        const auto late_begin = points.late_points.lower_bound(start);
        const auto late_finish = points.late_points.lower_bound(end);
        // Select context without materializing the entire sequence.  The
        // vector and the late-point tree are both ordered; merge only the
        // requested prefix/suffix and keep late corrections on equal times.
        if (query.preceding_points != 0) {
            auto vector_context = vector_begin;
            auto late_context = late_begin;
            std::vector<RawTimeseriesPoint> preceding;
            preceding.reserve(query.preceding_points);
            while (preceding.size() < query.preceding_points) {
                const bool has_vector = vector_context != active_begin;
                const bool has_late = late_context != points.late_points.begin();
                if (!has_vector && !has_late) {
                    break;
                }
                const auto* vector_point = has_vector
                    ? &*std::prev(vector_context)
                    : nullptr;
                const auto* late_point = has_late
                    ? &std::prev(late_context)->second
                    : nullptr;
                if (late_point != nullptr &&
                    (vector_point == nullptr ||
                     late_point->time >= vector_point->time)) {
                    preceding.push_back(*late_point);
                    --late_context;
                    if (vector_point != nullptr &&
                        vector_point->time == late_point->time) {
                        --vector_context;
                    }
                } else {
                    preceding.push_back(*vector_point);
                    --vector_context;
                }
            }
            std::reverse(preceding.begin(), preceding.end());
            for (auto& point : preceding) {
                output.push_back(std::move(point));
                ++count;
            }
        }
        auto vector_point = vector_begin;
        auto late_point = late_begin;
        while (vector_point != vector_finish || late_point != late_finish) {
            if (vector_point == vector_finish) {
                output.push_back(late_point->second);
                ++late_point;
            } else if (late_point == late_finish) {
                output.push_back(*vector_point);
                ++vector_point;
            } else if (vector_point->time < late_point->first) {
                output.push_back(*vector_point);
                ++vector_point;
            } else if (late_point->first < vector_point->time) {
                output.push_back(late_point->second);
                ++late_point;
            } else {
                // A late correction wins over the original vector point.
                output.push_back(late_point->second);
                ++vector_point;
                ++late_point;
            }
            ++count;
        }
        if (query.following_points != 0) {
            auto vector_context = vector_finish;
            auto late_context = late_finish;
            std::size_t following = 0;
            while (following < query.following_points) {
                const bool has_vector = vector_context != points.points.end();
                const bool has_late = late_context != points.late_points.end();
                if (!has_vector && !has_late) {
                    break;
                }
                const auto* vector_point = has_vector ? &*vector_context : nullptr;
                const auto* late_point = has_late ? &late_context->second : nullptr;
                if (late_point != nullptr &&
                    (vector_point == nullptr ||
                     late_point->time <= vector_point->time)) {
                    output.push_back(*late_point);
                    ++late_context;
                    if (vector_point != nullptr &&
                        vector_point->time == late_point->time) {
                        ++vector_context;
                    }
                } else {
                    output.push_back(*vector_point);
                    ++vector_context;
                }
                ++following;
                ++count;
            }
        }
        if (output.empty()) {
            result.data.sequence_values.erase(sequence_id);
        }
    }
    result.operation = internal::ok(count, "window query completed");
    return result;
}

}  // namespace sfkg::timeseries::core
