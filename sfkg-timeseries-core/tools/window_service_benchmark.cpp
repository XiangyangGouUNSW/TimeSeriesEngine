#include <algorithm>
#include <chrono>
#include <cstddef>
#include <iomanip>
#include <limits>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "sfkg/timeseries/core/window_service.hpp"

namespace core = sfkg::timeseries::core;
using Clock = std::chrono::steady_clock;

namespace {

struct BatchOptions {
    std::size_t sequence_count{7};
    std::size_t points_per_sequence{100};
    bool irregular{false};
    bool shuffled{false};
};

struct Measurement {
    double vector_ms{0.0};
    double map_ms{0.0};
    std::size_t points{0};
};

std::vector<core::SequenceId> sequenceIds(std::size_t count) {
    std::vector<core::SequenceId> ids;
    ids.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
        ids.push_back("window-bench-sequence-" + std::to_string(index));
    }
    return ids;
}

core::Timestamp timestampAt(std::size_t index, bool irregular) {
    // Irregular timestamps remain strictly increasing; no fixed sampling
    // interval is assumed by the benchmark or by WindowService.
    return 1'000'000 + static_cast<core::Timestamp>(index) * 1'000 +
        (irregular ? static_cast<core::Timestamp>((index % 17) * 13) : 0);
}

core::TimeseriesBatch makeBatchAt(
    const BatchOptions& options,
    std::size_t start_index) {
    const auto ids = sequenceIds(options.sequence_count);
    core::TimeseriesBatch batch;
    batch.points.reserve(
        options.sequence_count * options.points_per_sequence);
    for (std::size_t sequence = 0;
         sequence < options.sequence_count;
         ++sequence) {
        for (std::size_t index = 0;
             index < options.points_per_sequence;
             ++index) {
            const auto global_index = start_index + index;
            batch.points.push_back({
                timestampAt(global_index, options.irregular),
                ids[sequence],
                static_cast<double>(sequence) +
                    static_cast<double>(global_index) * 0.01});
        }
    }
    if (options.shuffled) {
        std::mt19937 generator(20260811);
        std::shuffle(batch.points.begin(), batch.points.end(), generator);
    }
    return batch;
}

core::TimeseriesBatch makeBatch(const BatchOptions& options) {
    return makeBatchAt(options, 0);
}

core::TimeseriesBatch makeCorrections(const BatchOptions& options) {
    const auto ids = sequenceIds(options.sequence_count);
    const auto correction_count = std::max<std::size_t>(
        1, options.points_per_sequence / 100);
    core::TimeseriesBatch batch;
    batch.points.reserve(options.sequence_count * correction_count);
    for (std::size_t sequence = 0;
         sequence < options.sequence_count;
         ++sequence) {
        for (std::size_t index = 0; index < correction_count; ++index) {
            const auto source_index = std::min(
                options.points_per_sequence - 1, index * 100);
            batch.points.push_back({
                timestampAt(source_index, options.irregular),
                ids[sequence],
                10'000.0 + static_cast<double>(source_index)});
        }
    }
    return batch;
}

double elapsedMs(Clock::time_point start, Clock::time_point end) {
    return std::chrono::duration<double, std::milli>(end - start).count();
}

double updateVector(const core::TimeseriesBatch& batch) {
    core::WindowService service;
    const auto configured = service.configureWindowSize(1'000'000'000);
    if (configured.code != core::OperationCode::Ok) {
        return -1.0;
    }
    const auto start = Clock::now();
    const auto result = service.buildTimeWindowIncremental(batch);
    const auto end = Clock::now();
    if (result.operation.code != core::OperationCode::Ok) {
        return -1.0;
    }
    return elapsedMs(start, end);
}

double updateMap(const core::TimeseriesBatch& batch) {
    const auto start = Clock::now();
    std::unordered_map<core::SequenceId,
                       std::map<core::Timestamp, core::RawTimeseriesPoint>>
        windows;
    std::unordered_map<core::SequenceId, std::size_t> incoming_counts;
    incoming_counts.reserve(batch.points.size());
    for (const auto& point : batch.points) {
        ++incoming_counts[point.sequence_id];
    }

    std::mutex mutex;
    std::lock_guard lock(mutex);
    const std::int64_t window_size = 1'000'000'000;
    std::optional<core::Timestamp> watermark;
    std::optional<core::Timestamp> affected_start;
    std::optional<core::Timestamp> affected_end;
    std::unordered_map<core::SequenceId, bool> changed;
    changed.reserve(batch.points.size());
    std::unordered_set<core::SequenceId> seen;
    seen.reserve(batch.points.size());

    for (const auto& point : batch.points) {
        seen.insert(point.sequence_id);
        affected_start = !affected_start
            ? std::optional<core::Timestamp>{point.time}
            : std::min(*affected_start, point.time);
        affected_end = !affected_end
            ? std::optional<core::Timestamp>{point.time}
            : std::max(*affected_end, point.time);
        auto& sequence = windows[point.sequence_id];
        if (sequence.find(point.time) != sequence.end() ||
            (!sequence.empty() && point.time < sequence.rbegin()->first)) {
            changed[point.sequence_id] = true;
        }
        sequence[point.time] = point;
        if (!watermark || point.time > *watermark) {
            watermark = point.time;
        }
    }

    if (watermark) {
        const auto start_time = *watermark <
                std::numeric_limits<core::Timestamp>::min() + window_size
            ? std::numeric_limits<core::Timestamp>::min()
            : *watermark - window_size;
        for (auto sequence = windows.begin();
             sequence != windows.end();) {
            sequence->second.erase(
                sequence->second.begin(),
                sequence->second.lower_bound(start_time));
            if (sequence->second.empty()) {
                sequence = windows.erase(sequence);
            } else {
                ++sequence;
            }
        }
    }
    const auto end = Clock::now();
    return elapsedMs(start, end);
}

Measurement measureScenario(const BatchOptions& options, bool corrections) {
    const auto batch = makeBatch(options);
    const auto points = corrections ? makeCorrections(options) :
                                      core::TimeseriesBatch{};

    Measurement measurement;
    measurement.points = corrections ? points.points.size() : batch.points.size();
    if (!corrections) {
        measurement.vector_ms = updateVector(batch);
        measurement.map_ms = updateMap(batch);
        return measurement;
    }

    core::WindowService vector_service;
    vector_service.configureWindowSize(1'000'000'000);
    vector_service.buildTimeWindowIncremental(batch);
    const auto vector_start = Clock::now();
    const auto vector_result = vector_service.buildTimeWindowIncremental(points);
    measurement.vector_ms = elapsedMs(vector_start, Clock::now());
    if (vector_result.operation.code != core::OperationCode::Ok) {
        measurement.vector_ms = -1.0;
    }

    std::unordered_map<core::SequenceId,
                       std::map<core::Timestamp, core::RawTimeseriesPoint>>
        map_windows;
    for (const auto& point : batch.points) {
        map_windows[point.sequence_id][point.time] = point;
    }
    const auto map_start = Clock::now();
    for (const auto& point : points.points) {
        map_windows[point.sequence_id][point.time] = point;
    }
    measurement.map_ms = elapsedMs(map_start, Clock::now());
    return measurement;
}

Measurement measureManyAppendBatches(
    const BatchOptions& options,
    std::size_t batch_count) {
    Measurement measurement;
    measurement.points = options.sequence_count *
        options.points_per_sequence * batch_count;

    core::WindowService vector_service;
    vector_service.configureWindowSize(1'000'000'000);
    const auto vector_start = Clock::now();
    for (std::size_t batch = 0; batch < batch_count; ++batch) {
        const auto input = makeBatchAt(
            options, batch * options.points_per_sequence);
        const auto result =
            vector_service.buildTimeWindowIncremental(input);
        if (result.operation.code != core::OperationCode::Ok) {
            measurement.vector_ms = -1.0;
            break;
        }
    }
    measurement.vector_ms = measurement.vector_ms < 0.0
        ? measurement.vector_ms
        : elapsedMs(vector_start, Clock::now());

    std::unordered_map<core::SequenceId,
                       std::map<core::Timestamp, core::RawTimeseriesPoint>>
        map_windows;
    const auto map_start = Clock::now();
    for (std::size_t batch = 0; batch < batch_count; ++batch) {
        const auto input = makeBatchAt(
            options, batch * options.points_per_sequence);
        for (const auto& point : input.points) {
            map_windows[point.sequence_id][point.time] = point;
        }
    }
    measurement.map_ms = elapsedMs(map_start, Clock::now());
    return measurement;
}

void printRow(
    const std::string& scenario,
    const BatchOptions& options,
    const Measurement& measurement) {
    const auto vector_rate = measurement.vector_ms > 0.0
        ? static_cast<double>(measurement.points) * 1'000.0 /
              measurement.vector_ms
        : 0.0;
    const auto map_rate = measurement.map_ms > 0.0
        ? static_cast<double>(measurement.points) * 1'000.0 /
              measurement.map_ms
        : 0.0;
    std::cout << std::left << std::setw(18) << scenario
              << " seq=" << std::setw(2) << options.sequence_count
              << " points_per_seq=" << std::setw(6)
              << options.points_per_sequence
              << " total_points=" << std::setw(7) << measurement.points
              << " vector_ms=" << std::setw(10) << std::fixed
              << std::setprecision(3) << measurement.vector_ms
              << " map_ms=" << std::setw(10) << measurement.map_ms
              << " vector_points_s=" << std::setw(12) << std::setprecision(0)
              << vector_rate
              << " map_points_s=" << map_rate << '\n';
}

void printManyBatchRow(
    const BatchOptions& options,
    std::size_t batch_count,
    const Measurement& measurement) {
    const auto vector_rate = measurement.vector_ms > 0.0
        ? static_cast<double>(measurement.points) * 1'000.0 /
              measurement.vector_ms
        : 0.0;
    const auto map_rate = measurement.map_ms > 0.0
        ? static_cast<double>(measurement.points) * 1'000.0 /
              measurement.map_ms
        : 0.0;
    std::cout << std::left << std::setw(18) << "many_small_batches"
              << " seq=" << std::setw(2) << options.sequence_count
              << " points_per_batch=" << std::setw(5)
              << options.points_per_sequence
              << " batches=" << std::setw(5) << batch_count
              << " total_points=" << std::setw(7) << measurement.points
              << " vector_ms=" << std::setw(10) << std::fixed
              << std::setprecision(3) << measurement.vector_ms
              << " map_ms=" << std::setw(10) << measurement.map_ms
              << " vector_points_s=" << std::setw(12) << std::setprecision(0)
              << vector_rate
              << " map_points_s=" << map_rate << '\n';
}

}  // namespace

int main() {
    std::cout << "WindowService vector-like benchmark\n"
              << "timestamps are per-point and may be irregular\n\n";

    for (const auto points_per_sequence : {100U, 1'000U, 5'000U}) {
        BatchOptions options;
        options.points_per_sequence = points_per_sequence;
        printRow("append", options, measureScenario(options, false));

        options.irregular = true;
        printRow("irregular_append", options, measureScenario(options, false));
    }

    // Keep the per-sequence batch size fixed while varying cardinality. This
    // distinguishes sequence fan-out from simply increasing one sequence's
    // history length.
    for (const auto sequence_count : {1U, 7U, 64U}) {
        BatchOptions options;
        options.sequence_count = sequence_count;
        options.points_per_sequence = 1'000;
        printRow("sequence_scale", options, measureScenario(options, false));
    }

    for (const auto points_per_sequence : {100U, 1'000U}) {
        BatchOptions options;
        options.points_per_sequence = points_per_sequence;
        options.shuffled = true;
        printRow("shuffled", options, measureScenario(options, false));
    }

    for (const auto points_per_sequence : {1'000U, 5'000U}) {
        BatchOptions options;
        options.points_per_sequence = points_per_sequence;
        options.irregular = true;
        printRow("late_corrections", options, measureScenario(options, true));
    }

    for (const auto batch_count : {100U, 1'000U}) {
        BatchOptions options;
        options.sequence_count = 7;
        options.points_per_sequence = 10;
        printManyBatchRow(
            options,
            batch_count,
            measureManyAppendBatches(options, batch_count));
    }
    return 0;
}
