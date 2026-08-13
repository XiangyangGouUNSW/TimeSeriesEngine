#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "sfkg/timeseries/core/grpc/ingest_task_executor.hpp"
#include "sfkg/timeseries/core/window_service.hpp"

namespace core = sfkg::timeseries::core;
namespace grpc_core = sfkg::timeseries::core::grpc;

int main() {
    setenv("SFKG_INGEST_COLD_WORKERS", "2", 1);
    setenv("SFKG_INGEST_HOT_WORKERS", "1", 1);
    setenv("SFKG_INGEST_QUEUE_CAPACITY", "1", 1);

    auto resolved = std::make_shared<core::IngestResult>();
    resolved->operation = {
        core::OperationCode::Ok, 2, 0, "resolved"};
    const std::string first_sequence = "executor-sequence-a";
    const std::string second_sequence = "executor-sequence-b";
    resolved->resolved_data.points.push_back({1, first_sequence, 1.0});
    resolved->resolved_data.points.push_back({2, second_sequence, 2.0});

    {
    grpc_core::IngestTaskExecutor executor;
    std::atomic<std::size_t> cold_callback_count{0};
    std::atomic<std::size_t> cold_point_count{0};
    const auto started = std::chrono::steady_clock::now();
    auto first = executor.trySubmit(
        resolved,
        [&](std::size_t, const core::TimeseriesBatch& data) {
            assert(!data.points.empty());
            for (const auto& point : data.points) {
                assert(point.sequence_id == data.points.front().sequence_id);
            }
            ++cold_callback_count;
            cold_point_count += data.points.size();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            return core::OperationResult{
                core::OperationCode::Ok, data.points.size(), 0, "cold"};
        },
        [](const core::TimeseriesBatch&) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            grpc_core::IngestPipelineResult result;
            result.window_result = {
                core::OperationCode::Ok, 2, 0, "hot"};
            result.derived_result = {
                core::OperationCode::Ok, 1, 0, "derived"};
            result.constraint_notification_result = {
                core::OperationCode::Ok, 0, 0, "notification"};
            return result;
        });
    assert(first.accepted);

    auto rejected = executor.trySubmit(
        resolved,
        [](std::size_t, const core::TimeseriesBatch&) {
            return core::OperationResult{
                core::OperationCode::Ok, 1, 0, "cold"};
        },
        [](const core::TimeseriesBatch&) {
            grpc_core::IngestPipelineResult result;
            result.window_result = {
                core::OperationCode::Ok, 2, 0, "hot"};
            result.derived_result = {
                core::OperationCode::Ok, 1, 0, "derived"};
            result.constraint_notification_result = {
                core::OperationCode::Ok, 0, 0, "notification"};
            return result;
        });
    assert(!rejected.accepted);
    assert(rejected.admission.code == core::OperationCode::Unavailable);

    const auto completed = first.completion.get();
    const auto elapsed_ms = std::chrono::duration_cast<
        std::chrono::milliseconds>(std::chrono::steady_clock::now() - started);
    assert(completed.storage_result.code == core::OperationCode::Ok);
    assert(completed.storage_result.success_count == 2);
    assert(cold_callback_count == 2);
    assert(cold_point_count == 2);
    assert(completed.window_result.code == core::OperationCode::Ok);
    assert(elapsed_ms.count() < 180);
    }

    // Multiple hot workers must not reverse two accepted batches that touch
    // the same sequence. The second callback is ready while the first one is
    // deliberately sleeping; its sequence dependency must keep it waiting.
    setenv("SFKG_INGEST_HOT_WORKERS", "2", 1);
    setenv("SFKG_INGEST_QUEUE_CAPACITY", "8", 1);
    grpc_core::IngestTaskExecutor ordered_executor;
    std::atomic<bool> first_hot_started{false};
    std::atomic<bool> first_hot_finished{false};
    std::atomic<bool> second_ran_before_first_finished{false};
    std::atomic<bool> first_incremental_safe{false};
    std::atomic<bool> second_incremental_safe{false};
    core::WindowService ordered_window;
    assert(ordered_window.configureWindowSize(1'000).code ==
           core::OperationCode::Ok);
    const auto make_resolved = [](core::Timestamp time) {
        auto value = std::make_shared<core::IngestResult>();
        value->operation = {core::OperationCode::Ok, 1, 0, "resolved"};
        value->resolved_data.points.push_back({
            time, "ordered-hot-sequence", static_cast<double>(time)});
        return value;
    };
    const auto cold_ok = [](std::size_t, const core::TimeseriesBatch& data) {
        return core::OperationResult{
            core::OperationCode::Ok, data.points.size(), 0, "cold"};
    };
    const auto hot_ok = [](std::size_t count) {
        grpc_core::IngestPipelineResult result;
        result.window_result = {
            core::OperationCode::Ok, count, 0, "hot"};
        result.derived_result = {
            core::OperationCode::Ok, 0, 0, "derived"};
        result.constraint_notification_result = {
            core::OperationCode::Ok, 0, 0, "notification"};
        return result;
    };

    auto ordered_first = ordered_executor.trySubmit(
        make_resolved(1),
        cold_ok,
        [&](const core::TimeseriesBatch& data) {
            first_hot_started = true;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            const auto update =
                ordered_window.buildTimeWindowIncremental(data);
            first_incremental_safe = update.incremental_safe;
            first_hot_finished = true;
            return hot_ok(data.points.size());
        });
    assert(ordered_first.accepted);
    while (!first_hot_started.load()) {
        std::this_thread::yield();
    }
    auto ordered_second = ordered_executor.trySubmit(
        make_resolved(2),
        cold_ok,
        [&](const core::TimeseriesBatch& data) {
            if (!first_hot_finished.load()) {
                second_ran_before_first_finished = true;
            }
            const auto update =
                ordered_window.buildTimeWindowIncremental(data);
            second_incremental_safe = update.incremental_safe;
            return hot_ok(data.points.size());
        });
    assert(ordered_second.accepted);
    assert(ordered_first.completion.get().window_result.code ==
           core::OperationCode::Ok);
    assert(ordered_second.completion.get().window_result.code ==
           core::OperationCode::Ok);
    assert(!second_ran_before_first_finished.load());
    assert(first_incremental_safe.load());
    assert(second_incremental_safe.load());

    std::cout << "ingest task executor test passed\n";
    return 0;
}
