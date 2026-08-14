#include <cassert>
#include <atomic>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "sfkg/timeseries/core/window_service.hpp"

using namespace sfkg::timeseries::core;

namespace {

const std::vector<RawTimeseriesPoint>& pointsFor(
    const WindowData& data,
    const SequenceId& sequence_id) {
    const auto found = data.sequence_values.find(sequence_id);
    assert(found != data.sequence_values.end());
    return found->second;
}

void assertTimes(
    const std::vector<RawTimeseriesPoint>& points,
    const std::vector<Timestamp>& expected) {
    assert(points.size() == expected.size());
    for (std::size_t index = 0; index < expected.size(); ++index) {
        assert(points[index].time == expected[index]);
    }
}

double pointValue(
    const std::vector<RawTimeseriesPoint>& points,
    Timestamp time) {
    for (const auto& point : points) {
        if (point.time == time) {
            return std::get<double>(point.value);
        }
    }
    assert(false);
    return 0.0;
}

}  // namespace

int main() {
    WindowService window;
    assert(window.configureWindowSize(10'000).code == OperationCode::Ok);

    // Irregular timestamps are valid. The container must preserve time order
    // without deriving timestamps from a sampling interval.
    const auto first = window.buildTimeWindowIncremental({{
        {1'000, "a", 1.0},
        {100, "b", 1.0},
        {1'750, "a", 2.0},
        {275, "b", 2.0},
        {3'000, "a", 3.0},
    }});
    assert(first.operation.code == OperationCode::Ok);
    assert(first.incremental_safe);
    assert(first.sequence_updates.at("a").incremental_safe);
    assert(first.sequence_updates.at("b").incremental_safe);
    auto queried = window.queryWindowData({
        {"a", "b"}, std::optional<Timestamp>{0},
        std::optional<Timestamp>{4'000}});
    assert(queried.operation.code == OperationCode::Ok);
    assertTimes(pointsFor(queried.data, "a"), {1'000, 1'750, 3'000});
    assertTimes(pointsFor(queried.data, "b"), {100, 275});

    // A late point is slower but remains correct and makes incremental
    // consumers fall back to a safe refresh path.
    const auto late = window.buildTimeWindowIncremental({{
        {1'250, "a", 12.5},
    }});
    assert(late.operation.code == OperationCode::Ok);
    assert(!late.incremental_safe);
    assert(!late.sequence_updates.at("a").incremental_safe);
    queried = window.queryWindowData({
        {"a"}, std::optional<Timestamp>{0},
        std::optional<Timestamp>{4'000}});
    assertTimes(
        pointsFor(queried.data, "a"),
        {1'000, 1'250, 1'750, 3'000});

    // A repeated timestamp overwrites the previous value and does not create
    // duplicate samples.
    const auto corrected = window.buildTimeWindowIncremental({{
        {1'750, "a", 17.5},
    }});
    assert(corrected.operation.code == OperationCode::Ok);
    assert(!corrected.incremental_safe);
    queried = window.queryWindowData({
        {"a"}, std::optional<Timestamp>{0},
        std::optional<Timestamp>{4'000}});
    assertTimes(
        pointsFor(queried.data, "a"),
        {1'000, 1'250, 1'750, 3'000});
    assert(pointValue(pointsFor(queried.data, "a"), 1'750) == 17.5);

    // Sparse corrections use the side tree temporarily, then are merged into
    // the ordered vector in one bounded batch instead of rebuilding it for
    // every correction.
    for (Timestamp time = 1'100; time < 1'164; ++time) {
        const auto correction = window.buildTimeWindowIncremental({{
            {time, "a", static_cast<double>(time)},
        }});
        assert(correction.operation.code == OperationCode::Ok);
        assert(!correction.incremental_safe);
    }
    queried = window.queryWindowData({
        {"a"}, std::optional<Timestamp>{0},
        std::optional<Timestamp>{4'000}});
    assert(pointsFor(queried.data, "a").size() == 68);
    assert(pointValue(pointsFor(queried.data, "a"), 1'163) == 1'163.0);

    // Advancing the watermark evicts old samples, but eviction is represented
    // independently so downstream consumers can patch the live boundary.
    WindowService advancing_window;
    assert(advancing_window.configureWindowSize(100).code ==
           OperationCode::Ok);
    assert(advancing_window.buildTimeWindowIncremental({{
        {0, "advancing", 0.0},
        {100, "advancing", 100.0},
    }}).incremental_safe);
    const auto evicting = advancing_window.buildTimeWindowIncremental({{
        {200, "advancing", 200.0},
    }});
    assert(evicting.operation.code == OperationCode::Ok);
    assert(evicting.incremental_safe);
    assert(evicting.window_evicted);
    assert(evicting.sequence_updates.at("advancing").incremental_safe);
    assert(evicting.sequence_updates.at("advancing").window_evicted);
    queried = advancing_window.queryWindowData({
        {"advancing"}, std::optional<Timestamp>{0},
        std::optional<Timestamp>{300}});
    assertTimes(pointsFor(queried.data, "advancing"), {100, 200});

    // A shuffled first batch is normalized once. Duplicate timestamps keep
    // the last value from the input batch, matching the old map assignment
    // semantics.
    WindowService shuffled_window;
    assert(shuffled_window.configureWindowSize(10'000).code ==
           OperationCode::Ok);
    const auto shuffled = shuffled_window.buildTimeWindowIncremental({{
        {300, "shuffled", 3.0},
        {100, "shuffled", 1.0},
        {200, "shuffled", 2.0},
        {200, "shuffled", 22.0},
    }});
    assert(shuffled.operation.code == OperationCode::Ok);
    assert(!shuffled.incremental_safe);
    queried = shuffled_window.queryWindowData({
        {"shuffled"}, std::optional<Timestamp>{0},
        std::optional<Timestamp>{400}});
    assertTimes(pointsFor(queried.data, "shuffled"), {100, 200, 300});
    assert(pointValue(pointsFor(queried.data, "shuffled"), 200) == 22.0);

    // Eviction uses real timestamps and keeps the inclusive left boundary.
    WindowService expiring;
    assert(expiring.configureWindowSize(1'000).code == OperationCode::Ok);
    assert(expiring.buildTimeWindow({{
        {1'000, "a", 1.0},
        {1'500, "a", 1.5},
        {2'000, "a", 2.0},
        {2'500, "a", 2.5},
    }}).code == OperationCode::Ok);
    queried = expiring.queryWindowData({
        {"a"}, std::optional<Timestamp>{2'000},
        std::optional<Timestamp>{2'501}});
    assertTimes(pointsFor(queried.data, "a"), {2'000, 2'500});
    queried = expiring.queryWindowData({
        {"a"}, std::optional<Timestamp>{0},
        std::optional<Timestamp>{1'500}});
    assert(queried.data.sequence_values.empty());

    // Derived replacement and range patching must also preserve sorted,
    // de-duplicated timestamps.
    WindowService derived_window;
    assert(derived_window.configureWindowSize(10'000).code ==
           OperationCode::Ok);
    // A real derived window is maintained alongside source data, so establish
    // a watermark before querying the derived sequence in this isolated test.
    assert(derived_window.buildTimeWindow({{
        {0, "source", 0.0},
    }}).code == OperationCode::Ok);
    assert(derived_window.replaceDerivedSequence(
        "derived", {{
            {300, "derived", 3.0},
            {100, "derived", 1.0},
            {200, "derived", 2.0},
            {200, "derived", 22.0},
        }}).code == OperationCode::Ok);
    queried = derived_window.queryWindowData({
        {"derived"}, std::optional<Timestamp>{0},
        std::optional<Timestamp>{400}});
    assertTimes(pointsFor(queried.data, "derived"), {100, 200, 300});
    assert(pointValue(pointsFor(queried.data, "derived"), 200) == 22.0);

    assert(derived_window.patchDerivedSequence(
        "derived", 200, 300, {{
            {250, "derived", 25.0},
        }}).code == OperationCode::Ok);
    queried = derived_window.queryWindowData({
        {"derived"}, std::optional<Timestamp>{0},
        std::optional<Timestamp>{400}});
    assertTimes(pointsFor(queried.data, "derived"), {100, 250});

    // A late derived point is merged before a range patch, so the patch does
    // not accidentally operate only on the append vector.
    assert(derived_window.buildTimeWindowIncremental({{
        {450, "source", 4.5},
    }}).operation.code == OperationCode::Ok);
    assert(derived_window.buildTimeWindowIncremental({{
        {150, "derived", 15.0},
    }}).operation.code == OperationCode::Ok);
    assert(derived_window.patchDerivedSequence(
        "derived", 100, 200, { {
            {175, "derived", 17.5},
        } }).code == OperationCode::Ok);
    queried = derived_window.queryWindowData({
        {"derived"}, std::optional<Timestamp>{0},
        std::optional<Timestamp>{500}});
    assertTimes(pointsFor(queried.data, "derived"), {175, 250});

    // Eviction advances a logical vector head and periodically compacts it;
    // the observable result is still exactly the timestamp window.
    WindowService compacting_window;
    assert(compacting_window.configureWindowSize(1'000).code ==
           OperationCode::Ok);
    TimeseriesBatch long_batch;
    long_batch.points.reserve(20'001);
    for (Timestamp time = 0; time <= 20'000; ++time) {
        long_batch.points.push_back({time, "compacting", static_cast<double>(time)});
    }
    assert(compacting_window.buildTimeWindow(long_batch).code ==
           OperationCode::Ok);
    queried = compacting_window.queryWindowData({
        {"compacting"}, std::optional<Timestamp>{19'000},
        std::optional<Timestamp>{20'001}});
    assert(pointsFor(queried.data, "compacting").size() == 1'001);
    assert(pointsFor(queried.data, "compacting").front().time == 19'000);
    assert(pointsFor(queried.data, "compacting").back().time == 20'000);

    // Concurrent callers are serialized only around the shared window index;
    // disjoint sequences remain independent and all data remains queryable.
    WindowService concurrent_window;
    assert(concurrent_window.configureWindowSize(1'000'000).code ==
           OperationCode::Ok);
    std::atomic<bool> concurrent_failure{false};
    std::vector<std::thread> writers;
    for (int worker = 0; worker < 4; ++worker) {
        writers.emplace_back([worker, &concurrent_window, &concurrent_failure] {
            TimeseriesBatch batch;
            batch.points.reserve(1'000);
            const auto sequence_id = "concurrent-" + std::to_string(worker);
            for (Timestamp time = 0; time < 1'000; ++time) {
                batch.points.push_back({
                    time, sequence_id, static_cast<double>(time)});
            }
            const auto result =
                concurrent_window.buildTimeWindowIncremental(batch);
            if (result.operation.code != OperationCode::Ok ||
                !result.incremental_safe) {
                concurrent_failure = true;
            }
        });
    }
    for (auto& writer : writers) {
        writer.join();
    }
    assert(!concurrent_failure.load());

    // The aggregate flag is false when one sequence is corrected, but an
    // unrelated append-only sequence remains independently safe.
    WindowService mixed_window;
    assert(mixed_window.configureWindowSize(10'000).code == OperationCode::Ok);
    assert(mixed_window.buildTimeWindowIncremental({{
        {0, "late", 0.0},
        {0, "append", 0.0},
    }}).incremental_safe);
    const auto mixed = mixed_window.buildTimeWindowIncremental({{
        {-1, "late", -1.0},
        {1, "append", 1.0},
    }});
    assert(!mixed.incremental_safe);
    assert(!mixed.sequence_updates.at("late").incremental_safe);
    assert(mixed.sequence_updates.at("append").incremental_safe);

    // One RPC may legitimately carry a wide variable set.  Its correctness
    // and per-sequence incremental status must not depend on the client
    // having pre-sharded the sequences into separate requests.
    WindowService wide_request_window;
    assert(wide_request_window.configureWindowSize(10'000).code ==
           OperationCode::Ok);
    TimeseriesBatch wide_first;
    TimeseriesBatch wide_second;
    wide_first.points.reserve(1'000);
    wide_second.points.reserve(1'000);
    for (int index = 0; index < 1'000; ++index) {
        const auto sequence_id = "wide-" + std::to_string(index);
        wide_first.points.push_back({
            0, sequence_id, static_cast<double>(index)});
        wide_second.points.push_back({
            1, sequence_id, static_cast<double>(index + 1)});
    }
    const auto wide_first_result =
        wide_request_window.buildTimeWindowIncremental(wide_first);
    const auto wide_second_result =
        wide_request_window.buildTimeWindowIncremental(wide_second);
    assert(wide_first_result.operation.code == OperationCode::Ok);
    assert(wide_first_result.incremental_safe);
    assert(wide_first_result.changed_sequence_ids.size() == 1'000);
    assert(wide_second_result.operation.code == OperationCode::Ok);
    assert(wide_second_result.incremental_safe);
    assert(wide_second_result.sequence_updates.size() == 1'000);
    assert(wide_second_result.sequence_updates.at("wide-0").incremental_safe);
    assert(wide_second_result.sequence_updates.at("wide-999").incremental_safe);
    queried = wide_request_window.queryWindowData({
        {"wide-0", "wide-500", "wide-999"},
        std::optional<Timestamp>{0}, std::optional<Timestamp>{2}});
    assertTimes(pointsFor(queried.data, "wide-0"), {0, 1});
    assertTimes(pointsFor(queried.data, "wide-500"), {0, 1});
    assertTimes(pointsFor(queried.data, "wide-999"), {0, 1});

    for (int worker = 0; worker < 4; ++worker) {
        const auto sequence_id = "concurrent-" + std::to_string(worker);
        queried = concurrent_window.queryWindowData({
            {sequence_id}, std::optional<Timestamp>{0},
            std::optional<Timestamp>{1'000}});
        assert(pointsFor(queried.data, sequence_id).size() == 1'000);
    }

    std::cout << "window_service_test passed\n";
    return 0;
}
