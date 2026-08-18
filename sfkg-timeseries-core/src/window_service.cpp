#include "sfkg/timeseries/core/window_service.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <functional>
#include <future>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_set>
#include <utility>

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core {

namespace {

std::size_t sequenceExecutorWorkerCount() {
    constexpr std::size_t kDefaultWorkers = 8;
    constexpr std::size_t kMaxWorkers = 64;
    const char* value = std::getenv("SFKG_INGEST_HOT_SEQUENCE_WORKERS");
    if (value != nullptr && *value != '\0') {
        char* end = nullptr;
        const auto parsed = std::strtoull(value, &end, 10);
        if (end != value && *end == '\0' && parsed > 0) {
            return std::min<std::size_t>(
                kMaxWorkers, static_cast<std::size_t>(parsed));
        }
    }
    return kDefaultWorkers;
}

constexpr std::size_t kSequenceTaskBaseCost = 1;
constexpr std::size_t kSequenceTaskPointCost = 1;
constexpr std::size_t kSequenceTaskCorrectionCost = 4;
constexpr std::size_t kSequenceTaskOversubscription = 2;
constexpr std::size_t kMinimumSequenceGroupCost = 8;

ProjectId effectiveProject(const ProjectId& project_id) {
    return project_id.empty() ? ProjectId{"default"} : project_id;
}

std::string scopedSequenceKey(
    const ProjectId& project_id,
    const SequenceId& sequence_id) {
    return effectiveProject(project_id) + '\x1e' + sequence_id;
}

template <typename Item, typename CostFunction>
std::vector<std::vector<Item>> groupSequenceTasks(
    std::vector<Item> items,
    std::size_t worker_count,
    CostFunction cost_function) {
    if (items.empty()) {
        return {};
    }
    if (worker_count == 0) {
        return {std::move(items)};
    }

    std::size_t total_cost = 0;
    for (const auto& item : items) {
        total_cost += cost_function(item);
    }
    const auto denominator = worker_count * kSequenceTaskOversubscription;
    const auto calculated_target = (total_cost + denominator - 1) /
        denominator;
    const auto target_cost = std::max(
        kMinimumSequenceGroupCost,
        std::max<std::size_t>(1, calculated_target));

    // Longest-processing-time-first keeps a large sequence from being
    // hidden behind a group of many small sequences. A sequence itself is
    // never split; only independent small sequences are coalesced.
    std::stable_sort(
        items.begin(),
        items.end(),
        [&cost_function](const Item& left, const Item& right) {
            return cost_function(left) > cost_function(right);
        });

    std::vector<std::vector<Item>> groups;
    std::vector<std::size_t> group_costs;
    for (auto& item : items) {
        const auto item_cost = cost_function(item);
        std::optional<std::size_t> best_group;
        for (std::size_t index = 0; index < group_costs.size(); ++index) {
            if (group_costs[index] + item_cost > target_cost) {
                continue;
            }
            if (!best_group ||
                group_costs[index] < group_costs[*best_group]) {
                best_group = index;
            }
        }
        if (!best_group) {
            groups.emplace_back();
            group_costs.push_back(0);
            best_group = groups.size() - 1;
        }
        groups[*best_group].push_back(std::move(item));
        group_costs[*best_group] += item_cost;
    }
    return groups;
}

}  // namespace

struct WindowService::SequenceExecutor {
    explicit SequenceExecutor(std::size_t worker_count) {
        workers.reserve(worker_count);
        for (std::size_t index = 0; index < worker_count; ++index) {
            workers.emplace_back([this] { run(); });
        }
    }

    ~SequenceExecutor() {
        {
            std::lock_guard lock(mutex);
            stopping = true;
        }
        condition.notify_all();
        for (auto& worker : workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    template <typename Function>
    auto submit(Function function) ->
        std::future<std::invoke_result_t<Function>> {
        using Result = std::invoke_result_t<Function>;
        auto task = std::make_shared<std::packaged_task<Result()>>(
            std::move(function));
        auto future = task->get_future();
        {
            std::lock_guard lock(mutex);
            if (stopping) {
                throw std::runtime_error("sequence executor is stopping");
            }
            queue.emplace_back([task = std::move(task)]() mutable {
                (*task)();
            });
        }
        condition.notify_one();
        return future;
    }

    std::size_t workerCount() const {
        return workers.size();
    }

private:
    void run() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock lock(mutex);
                condition.wait(lock, [this] {
                    return stopping || !queue.empty();
                });
                if (stopping && queue.empty()) {
                    return;
                }
                task = std::move(queue.front());
                queue.pop_front();
            }
            task();
        }
    }

    std::mutex mutex;
    std::condition_variable condition;
    std::deque<std::function<void()>> queue;
    std::vector<std::thread> workers;
    bool stopping{false};
};

WindowService::WindowService()
    : sequence_executor_(
          std::make_unique<SequenceExecutor>(sequenceExecutorWorkerCount())) {}

WindowService::~WindowService() = default;

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

std::int64_t WindowService::windowSize(const ProjectId& project_id) const {
    std::shared_lock lock(mutex_);
    const auto found = window_sizes_.find(effectiveProject(project_id));
    return found == window_sizes_.end() ? kDefaultWindowSizeMs : found->second;
}

std::int64_t WindowService::windowSize() const {
    return windowSize("default");
}

std::shared_ptr<WindowService::SequenceWindow>
WindowService::sequenceWindowFor(
    const ProjectId& project_id,
    const SequenceId& sequence_id) {
    std::unique_lock lock(mutex_);
    auto& sequence = sequence_windows_[scopedSequenceKey(project_id, sequence_id)];
    if (!sequence) {
        sequence = std::make_shared<SequenceWindow>();
    }
    return sequence;
}

OperationResult WindowService::configureWindowSize(
    const ProjectId& project_id,
    std::int64_t window_size) {
    const auto scoped_project = effectiveProject(project_id);
    if (window_size <= 0) {
        return internal::invalidArgument(
            "window_size must be positive");
    }

    std::vector<std::pair<SequenceId, std::shared_ptr<SequenceWindow>>>
        sequences;
    Timestamp window_start = 0;
    bool has_watermark = false;
    {
        std::unique_lock lock(mutex_);
        window_sizes_[scoped_project] = window_size;
        const auto watermark = watermarks_.find(scoped_project);
        has_watermark = watermark != watermarks_.end();
        if (has_watermark) {
            window_start = watermark->second <
                    std::numeric_limits<Timestamp>::min() + window_size
                ? std::numeric_limits<Timestamp>::min()
                : watermark->second - window_size;
        }
        sequences.reserve(sequence_windows_.size());
        const auto prefix = scoped_project + '\x1e';
        for (const auto& [sequence_key, sequence] : sequence_windows_) {
            if (sequence_key.rfind(prefix, 0) == 0) {
                sequences.emplace_back(sequence_key, sequence);
            }
        }
    }
    if (!sequences.empty() && has_watermark) {
        (void)pruneExpiredPoints(scoped_project, window_start, sequences);
    }
    return internal::ok(0, "hot window size configured");
}

OperationResult WindowService::configureWindowSize(std::int64_t window_size) {
    return configureWindowSize("default", window_size);
}

OperationResult WindowService::buildTimeWindow(
    const TimeseriesBatch& data) {
    return updateWindowIncremental(data.project_id, data, std::nullopt).operation;
}

OperationResult WindowService::buildTimeWindow(
    const ProjectId& project_id,
    const TimeseriesBatch& data) {
    return updateWindowIncremental(project_id, data, std::nullopt).operation;
}

OperationResult WindowService::buildTimeWindow(
    const ProjectId& project_id,
    const TimeseriesBatch& data,
    std::int64_t window_size) {
    return updateWindowIncremental(project_id, data, window_size).operation;
}

OperationResult WindowService::buildTimeWindow(
    const TimeseriesBatch& data,
    std::int64_t window_size) {
    return buildTimeWindow(data.project_id, data, window_size);
}

WindowUpdateResult WindowService::buildTimeWindowIncremental(
    const ProjectId& project_id,
    const TimeseriesBatch& data) {
    return updateWindowIncremental(project_id, data, std::nullopt);
}

WindowUpdateResult WindowService::buildTimeWindowIncremental(
    const TimeseriesBatch& data) {
    return buildTimeWindowIncremental(data.project_id, data);
}

OperationResult WindowService::replaceDerivedSequence(
    const ProjectId& project_id,
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

    const auto output = sequenceWindowFor(project_id, sequence_id);
    {
        std::unique_lock lock(output->mutex);
        replaceSequence(*output, data);
    }
    std::vector<std::pair<SequenceId, std::shared_ptr<SequenceWindow>>>
        sequences;
    Timestamp window_start = 0;
    bool has_watermark = false;
    {
        std::shared_lock lock(mutex_);
        const auto watermark = watermarks_.find(effectiveProject(project_id));
        has_watermark = watermark != watermarks_.end();
        if (has_watermark) {
            const auto window_size = windowSize(project_id);
            window_start = watermark->second <
                    std::numeric_limits<Timestamp>::min() + window_size
                ? std::numeric_limits<Timestamp>::min()
                : watermark->second - window_size;
        }
        const auto prefix = effectiveProject(project_id) + '\x1e';
        for (const auto& [id, sequence] : sequence_windows_) {
            if (id.rfind(prefix, 0) == 0) {
                sequences.emplace_back(id, sequence);
            }
        }
    }
    if (has_watermark) {
        (void)pruneExpiredPoints(project_id, window_start, sequences);
    }
    return internal::ok(data.points.size(), "derived hot window replaced");
}

OperationResult WindowService::replaceDerivedSequence(
    const SequenceId& sequence_id,
    const TimeseriesBatch& data) {
    return replaceDerivedSequence(data.project_id, sequence_id, data);
}

OperationResult WindowService::patchDerivedSequence(
    const ProjectId& project_id,
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

    const auto output = sequenceWindowFor(project_id, sequence_id);
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

OperationResult WindowService::patchDerivedSequence(
    const SequenceId& sequence_id,
    Timestamp start_time,
    Timestamp end_time,
    const TimeseriesBatch& data) {
    return patchDerivedSequence(
        data.project_id, sequence_id, start_time, end_time, data);
}

OperationResult WindowService::updateWindow(
    const ProjectId& project_id,
    const TimeseriesBatch& data,
    std::optional<std::int64_t> window_size_override) {
    return updateWindowIncremental(project_id, data, window_size_override).operation;
}

OperationResult WindowService::updateWindow(
    const TimeseriesBatch& data,
    std::optional<std::int64_t> window_size_override) {
    return updateWindow(data.project_id, data, window_size_override);
}

WindowUpdateResult WindowService::updateWindowIncremental(
    const ProjectId& project_id,
    const TimeseriesBatch& data,
    std::optional<std::int64_t> window_size_override) {
    const auto scoped_project = effectiveProject(project_id);
    WindowUpdateResult result;
    result.project_id = scoped_project;
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
        auto& sequence_update = result.sequence_updates[point.sequence_id];
        if (!sequence_update.affected_start_time ||
            point.time < *sequence_update.affected_start_time) {
            sequence_update.affected_start_time = point.time;
        }
        if (!sequence_update.affected_end_time ||
            point.time > *sequence_update.affected_end_time) {
            sequence_update.affected_end_time = point.time;
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

    std::vector<std::pair<SequenceId, std::shared_ptr<SequenceWindow>>>
        all_sequences;
    std::unordered_map<SequenceId, std::shared_ptr<SequenceWindow>>
        sequence_refs;
    Timestamp window_start = 0;
    {
        std::unique_lock lock(mutex_);
        if (window_size_override) {
            window_sizes_[scoped_project] = *window_size_override;
        }
        const bool had_watermark =
            watermarks_.find(scoped_project) != watermarks_.end();
        auto& watermark = watermarks_[scoped_project];
        for (const auto& point : data.points) {
            if (!had_watermark ||
                point.time > watermark) {
                watermark = point.time;
            }
        }
        auto& update_generation = update_generations_[scoped_project];
        if (update_generation != std::numeric_limits<std::uint64_t>::max()) {
            ++update_generation;
        }
        result.update_generation = update_generation;
        const auto window_size = window_sizes_.count(scoped_project) != 0
            ? window_sizes_.at(scoped_project)
            : kDefaultWindowSizeMs;
        if (watermarks_.find(scoped_project) != watermarks_.end()) {
            window_start = watermark <
                    std::numeric_limits<Timestamp>::min() + window_size
                ? std::numeric_limits<Timestamp>::min()
                : watermark - window_size;
            result.window_start_time = window_start;
        }
        sequence_refs.reserve(incoming_by_sequence.size());
        for (const auto& [sequence_id, points] : incoming_by_sequence) {
            (void)points;
            auto& sequence = sequence_windows_[
                scopedSequenceKey(scoped_project, sequence_id)];
            if (!sequence) {
                sequence = std::make_shared<SequenceWindow>();
            }
            sequence_refs.emplace(sequence_id, sequence);
        }
        const auto prefix = scoped_project + '\x1e';
        for (const auto& [sequence_key, sequence] : sequence_windows_) {
            if (sequence_key.rfind(prefix, 0) == 0) {
                all_sequences.emplace_back(
                    sequence_key.substr(prefix.size()), sequence);
            }
        }
    }

    result.incremental_safe = true;
    struct PreparedSequenceUpdate {
        SequenceId sequence_id;
        std::shared_ptr<SequenceWindow> sequence;
        std::vector<RawTimeseriesPoint> incoming;
        bool strictly_increasing{false};
        std::size_t estimated_cost{0};
    };
    struct SequenceUpdateTiming {
        SequenceId sequence_id;
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
        const auto estimated_cost =
            kSequenceTaskBaseCost +
            incoming.size() * kSequenceTaskPointCost +
            (strictly_increasing
                 ? 0
                 : incoming.size() * kSequenceTaskCorrectionCost);
        prepared_updates.push_back({
            sequence_id,
            sequence_refs.at(sequence_id),
            std::move(incoming),
            strictly_increasing,
            estimated_cost});
    }

    result.sequence_task_count = prepared_updates.size();
    auto sequence_groups = groupSequenceTasks(
        std::move(prepared_updates),
        sequence_executor_->workerCount(),
        [](const PreparedSequenceUpdate& prepared) {
            return prepared.estimated_cost;
        });
    result.sequence_group_count = sequence_groups.size();

    const auto update_sequence = [](PreparedSequenceUpdate prepared) {
        SequenceUpdateTiming timing;
        timing.sequence_id = prepared.sequence_id;
        const auto wait_started = std::chrono::steady_clock::now();
        std::unique_lock lock(prepared.sequence->mutex);
        const auto lock_acquired = std::chrono::steady_clock::now();
        timing.lock_wait_ms =
            std::chrono::duration<double, std::milli>(
                lock_acquired - wait_started).count();

        const auto has_active_points = !prepared.sequence->empty();
        const bool append_only = prepared.strictly_increasing &&
            !prepared.incoming.empty() &&
            (!prepared.sequence->latest_time ||
             prepared.incoming.front().time >
                 *prepared.sequence->latest_time);
        const auto update_started = lock_acquired;
        if (append_only) {
            const auto required_capacity =
                prepared.sequence->points.size() + prepared.incoming.size();
            if (prepared.sequence->points.capacity() < required_capacity) {
                const auto doubled_capacity =
                    prepared.sequence->points.capacity() >
                            std::numeric_limits<std::size_t>::max() / 2
                        ? std::numeric_limits<std::size_t>::max()
                        : prepared.sequence->points.capacity() * 2;
                prepared.sequence->points.reserve(std::max(
                    required_capacity,
                    std::max<std::size_t>(1, doubled_capacity)));
            }
            for (auto& point : prepared.incoming) {
                prepared.sequence->points.push_back(std::move(point));
                prepared.sequence->latest_time =
                    prepared.sequence->points.back().time;
            }
            timing.incremental_safe = true;
        } else if (!has_active_points &&
                   prepared.sequence->late_points.empty()) {
            WindowService::mergeSortedPoints(
                *prepared.sequence, std::move(prepared.incoming));
        } else {
            for (auto& point : prepared.incoming) {
                const auto point_time = point.time;
                prepared.sequence->late_points[point.time] = std::move(point);
                if (!prepared.sequence->latest_time ||
                    point_time > *prepared.sequence->latest_time) {
                    prepared.sequence->latest_time = point_time;
                }
            }
            if (prepared.sequence->late_points.size() >=
                WindowService::kLatePointFlushThreshold) {
                // Corrections remain cheap while sparse, but the side buffer
                // is periodically folded back into the append-oriented
                // vector so it cannot grow without bound.
                WindowService::flushLatePoints(*prepared.sequence);
            }
        }
        timing.update_ms =
            std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - update_started).count();
        return timing;
    };

    std::vector<std::future<std::vector<SequenceUpdateTiming>>> updates;
    updates.reserve(sequence_groups.size());
    for (auto& group : sequence_groups) {
        updates.push_back(sequence_executor_->submit(
            [group = std::move(group), update_sequence]() mutable {
                std::vector<SequenceUpdateTiming> timings;
                timings.reserve(group.size());
                for (auto& prepared : group) {
                    timings.push_back(update_sequence(std::move(prepared)));
                }
                return timings;
            }));
    }
    for (auto& update : updates) {
        for (const auto& timing : update.get()) {
            result.incremental_safe =
                timing.incremental_safe && result.incremental_safe;
            result.sequence_updates[timing.sequence_id].incremental_safe =
                timing.incremental_safe;
            result.sequence_lock_wait_ms += timing.lock_wait_ms;
            result.sequence_update_ms += timing.update_ms;
        }
    }

    // Eviction is not an ordering failure. It is removed independently while
    // the per-sequence update work above can run concurrently.
    std::unordered_set<SequenceId> evicted_sequence_ids;
    result.window_evicted = pruneExpiredPoints(
        scoped_project,
        window_start,
        all_sequences,
        &evicted_sequence_ids,
        &result.eviction_lock_wait_ms,
        &result.eviction_update_ms);
    for (const auto& sequence_id : result.changed_sequence_ids) {
        result.sequence_updates[sequence_id].window_evicted =
            evicted_sequence_ids.find(sequence_id) !=
            evicted_sequence_ids.end();
    }
    for (const auto& sequence_id : evicted_sequence_ids) {
        auto& sequence_update = result.sequence_updates[sequence_id];
        sequence_update.window_evicted = true;
        if (!sequence_update.affected_start_time) {
            sequence_update.affected_start_time = window_start;
        }
        if (!sequence_update.affected_end_time) {
            sequence_update.affected_end_time =
                result.affected_end_time.value_or(window_start);
        }
        if (std::find(
                result.changed_sequence_ids.begin(),
                result.changed_sequence_ids.end(),
                sequence_id) == result.changed_sequence_ids.end()) {
            // Eviction changes the live window even when this request did not
            // carry a new point for the sequence.
            result.changed_sequence_ids.push_back(sequence_id);
        }
    }
    result.operation = internal::ok(
        data.points.size(), "hot window updated");
    return result;
}

bool WindowService::pruneExpiredPoints(
    const ProjectId& project_id,
    Timestamp start,
    const std::vector<std::pair<SequenceId, std::shared_ptr<SequenceWindow>>>&
        sequences,
    std::unordered_set<SequenceId>* evicted_sequence_ids,
    double* lock_wait_ms,
    double* update_ms) {
    if (sequences.empty()) {
        return false;
    }
    (void)project_id;

    // Eviction touches independent per-sequence buffers.  Keep the global
    // metadata lock out of this phase and let each sequence use its own lock;
    // this is safe because callers pass a stable snapshot of shared_ptrs.
    struct PruneTiming {
        bool evicted{false};
        double lock_wait_ms{0.0};
        double update_ms{0.0};
    };
    auto pruneOne = [start](
                        const std::pair<SequenceId,
                                        std::shared_ptr<SequenceWindow>>& entry) {
        PruneTiming timing;
        const auto wait_started = std::chrono::steady_clock::now();
        bool evicted = false;
        const auto& sequence = entry.second;
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

    // For a small sequence set, queueing work is more expensive than the
    // lower_bound calls. The threshold is based only on executor overhead,
    // not on a particular dataset or workload shape.
    constexpr std::size_t kParallelPruneThreshold = 16;
    if (sequences.size() < kParallelPruneThreshold) {
        bool evicted = false;
        for (const auto& entry : sequences) {
            const auto timing = pruneOne(entry);
            evicted = timing.evicted || evicted;
            if (timing.evicted && evicted_sequence_ids != nullptr) {
                evicted_sequence_ids->insert(entry.first);
            }
            if (lock_wait_ms != nullptr) {
                *lock_wait_ms += timing.lock_wait_ms;
            }
            if (update_ms != nullptr) {
                *update_ms += timing.update_ms;
            }
        }
        return evicted;
    }

    auto sequence_groups = groupSequenceTasks(
        std::vector<std::pair<SequenceId, std::shared_ptr<SequenceWindow>>>(
            sequences.begin(), sequences.end()),
        sequence_executor_->workerCount(),
        [](const auto&) { return std::size_t{1}; });
    using PruneResult = std::pair<SequenceId, PruneTiming>;
    std::vector<std::future<std::vector<PruneResult>>> futures;
    futures.reserve(sequence_groups.size());
    for (auto& group : sequence_groups) {
        futures.push_back(sequence_executor_->submit(
            [pruneOne, group = std::move(group)]() mutable {
                std::vector<PruneResult> results;
                results.reserve(group.size());
                for (const auto& entry : group) {
                    results.emplace_back(entry.first, pruneOne(entry));
                }
                return results;
            }));
    }
    bool evicted = false;
    for (auto& future : futures) {
        for (const auto& [sequence_id, timing] : future.get()) {
            evicted = timing.evicted || evicted;
            if (timing.evicted && evicted_sequence_ids != nullptr) {
                evicted_sequence_ids->insert(sequence_id);
            }
            if (lock_wait_ms != nullptr) {
                *lock_wait_ms += timing.lock_wait_ms;
            }
            if (update_ms != nullptr) {
                *update_ms += timing.update_ms;
            }
        }
    }
    return evicted;
}

WindowQueryResult WindowService::queryWindowData(
    const ProjectId& project_id,
    const WindowQuery& query) const {
    WindowQueryResult result;
    result.project_id = effectiveProject(project_id);
    result.data.project_id = result.project_id;
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
        const auto watermark_found = watermarks_.find(result.project_id);
        if (watermark_found != watermarks_.end()) {
            watermark = watermark_found->second;
        }
        const auto size_found = window_sizes_.find(result.project_id);
        window_size = size_found == window_sizes_.end()
            ? kDefaultWindowSizeMs
            : size_found->second;
        sequences.reserve(query.sequence_ids.size());
        for (const auto& sequence_id : query.sequence_ids) {
            const auto found = sequence_windows_.find(
                scopedSequenceKey(result.project_id, sequence_id));
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

WindowQueryResult WindowService::queryWindowData(
    const WindowQuery& query) const {
    return queryWindowData(query.project_id, query);
}

}  // namespace sfkg::timeseries::core
