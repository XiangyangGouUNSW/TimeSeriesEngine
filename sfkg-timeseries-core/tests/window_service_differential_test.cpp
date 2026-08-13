#include <cassert>
#include <iostream>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "sfkg/timeseries/core/window_service.hpp"

namespace core = sfkg::timeseries::core;

namespace {

using ReferenceWindow = std::map<core::SequenceId,
                                 std::map<core::Timestamp, double>>;

void pruneReference(
    ReferenceWindow& reference,
    std::optional<core::Timestamp> watermark,
    core::Timestamp window_size) {
    if (!watermark) {
        return;
    }
    const auto start = *watermark - window_size;
    for (auto sequence = reference.begin(); sequence != reference.end();) {
        sequence->second.erase(
            sequence->second.begin(),
            sequence->second.lower_bound(start));
        if (sequence->second.empty()) {
            sequence = reference.erase(sequence);
        } else {
            ++sequence;
        }
    }
}

double valueOf(const core::RawTimeseriesPoint& point) {
    return std::get<double>(point.value);
}

void compareQuery(
    core::WindowService& service,
    const ReferenceWindow& reference,
    const std::vector<core::SequenceId>& sequence_ids,
    core::Timestamp start,
    core::Timestamp end) {
    const auto result = service.queryWindowData({
        sequence_ids,
        std::optional<core::Timestamp>{start},
        std::optional<core::Timestamp>{end}});
    assert(result.operation.code == core::OperationCode::Ok);

    std::size_t expected_count = 0;
    for (const auto& sequence_id : sequence_ids) {
        const auto expected = reference.find(sequence_id);
        const auto actual = result.data.sequence_values.find(sequence_id);
        if (expected == reference.end()) {
            assert(actual == result.data.sequence_values.end());
            continue;
        }

        std::vector<std::pair<core::Timestamp, double>> expected_points;
        for (const auto& [time, value] : expected->second) {
            if (time >= start && time < end) {
                expected_points.emplace_back(time, value);
            }
        }
        if (expected_points.empty()) {
            assert(actual == result.data.sequence_values.end());
            continue;
        }
        assert(actual != result.data.sequence_values.end());
        assert(actual->second.size() == expected_points.size());
        expected_count += expected_points.size();
        for (std::size_t index = 0; index < expected_points.size(); ++index) {
            assert(actual->second[index].time == expected_points[index].first);
            assert(valueOf(actual->second[index]) == expected_points[index].second);
        }
    }
    assert(result.operation.success_count == expected_count);
    // Before the first ingest WindowService returns an empty result without
    // a meaningful window range; the reference has no watermark either.
    if (reference.empty() &&
        result.data.window_start_time == 0 &&
        result.data.window_end_time == 0) {
        return;
    }
    assert(result.data.window_start_time == start);
    assert(result.data.window_end_time == end);
}

}  // namespace

int main() {
    // Basic validation paths are checked separately from the randomized run.
    core::WindowService invalid_service;
    assert(invalid_service.configureWindowSize(0).code ==
           core::OperationCode::InvalidArgument);
    assert(invalid_service.buildTimeWindow({}).code ==
           core::OperationCode::InvalidArgument);
    assert(invalid_service.buildTimeWindow({{
        {1, "", 1.0},
    }}).code == core::OperationCode::InvalidArgument);
    assert(invalid_service.queryWindowData({{}, 0, 1}).operation.code ==
           core::OperationCode::InvalidArgument);
    assert(invalid_service.queryWindowData({{"s0"}, 2, 1}).operation.code ==
           core::OperationCode::InvalidArgument);

    core::WindowService service;
    constexpr core::Timestamp initial_window_size = 137;
    assert(service.configureWindowSize(initial_window_size).code ==
           core::OperationCode::Ok);

    ReferenceWindow reference;
    std::optional<core::Timestamp> watermark;
    core::Timestamp window_size = initial_window_size;
    const std::vector<core::SequenceId> sequence_ids{
        "random-0", "random-1", "random-2", "random-3", "random-4"};
    std::mt19937 generator(20260811);
    std::uniform_int_distribution<int> operation(0, 99);
    std::uniform_int_distribution<int> sequence_index(0, 4);
    std::uniform_int_distribution<int> time_distribution(-100, 900);
    std::uniform_int_distribution<int> value_distribution(-10'000, 10'000);

    for (int round = 0; round < 3'000; ++round) {
        const auto operation_kind = operation(generator);
        if (operation_kind < 72) {
            std::uniform_int_distribution<int> count_distribution(1, 25);
            const auto count = count_distribution(generator);
            core::TimeseriesBatch batch;
            batch.points.reserve(static_cast<std::size_t>(count));
            for (int index = 0; index < count; ++index) {
                const auto sequence_id = sequence_ids[
                    static_cast<std::size_t>(sequence_index(generator))];
                const auto time = static_cast<core::Timestamp>(
                    time_distribution(generator));
                batch.points.push_back({
                    time,
                    sequence_id,
                    static_cast<double>(value_distribution(generator))});
            }

            const auto result = service.buildTimeWindowIncremental(batch);
            assert(result.operation.code == core::OperationCode::Ok);
            assert(result.operation.success_count == batch.points.size());
            for (const auto& point : batch.points) {
                reference[point.sequence_id][point.time] = valueOf(point);
                if (!watermark || point.time > *watermark) {
                    watermark = point.time;
                }
            }
            pruneReference(reference, watermark, window_size);
        } else if (operation_kind < 84) {
            std::uniform_int_distribution<int> size_distribution(1, 250);
            window_size = size_distribution(generator);
            assert(service.configureWindowSize(window_size).code ==
                   core::OperationCode::Ok);
            pruneReference(reference, watermark, window_size);
        } else {
            const auto start = static_cast<core::Timestamp>(
                time_distribution(generator));
            const auto end = start +
                static_cast<core::Timestamp>(
                    1 + (operation(generator) % 250));
            compareQuery(service, reference, sequence_ids, start, end);
        }

        // Compare a broad range regularly, including after writes and window
        // size changes. This catches eviction and duplicate replacement bugs
        // that a single final query would miss.
        if (round % 17 == 0) {
            compareQuery(service, reference, sequence_ids, -1'000, 2'000);
        }
    }

    compareQuery(service, reference, sequence_ids, -1'000, 2'000);
    std::cout << "window_service_differential_test passed\n";
    return 0;
}
