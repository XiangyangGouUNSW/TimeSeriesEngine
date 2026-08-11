#include <cassert>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <thread>

#include "sfkg/timeseries/core/grpc/ingest_task_executor.hpp"

namespace core = sfkg::timeseries::core;
namespace grpc_core = sfkg::timeseries::core::grpc;

int main() {
    setenv("SFKG_INGEST_COLD_WORKERS", "1", 1);
    setenv("SFKG_INGEST_HOT_WORKERS", "1", 1);
    setenv("SFKG_INGEST_QUEUE_CAPACITY", "1", 1);

    auto resolved = std::make_shared<core::IngestResult>();
    resolved->operation = {
        core::OperationCode::Ok, 1, 0, "resolved"};
    resolved->resolved_data.points.push_back({1, "executor-sequence", 1.0});

    grpc_core::IngestTaskExecutor executor;
    const auto started = std::chrono::steady_clock::now();
    auto first = executor.trySubmit(
        resolved,
        [](const core::TimeseriesBatch&) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            return core::OperationResult{
                core::OperationCode::Ok, 1, 0, "cold"};
        },
        [](const core::TimeseriesBatch&) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            grpc_core::IngestPipelineResult result;
            result.window_result = {
                core::OperationCode::Ok, 1, 0, "hot"};
            result.derived_result = {
                core::OperationCode::Ok, 1, 0, "derived"};
            result.constraint_notification_result = {
                core::OperationCode::Ok, 0, 0, "notification"};
            return result;
        });
    assert(first.accepted);

    auto rejected = executor.trySubmit(
        resolved,
        [](const core::TimeseriesBatch&) {
            return core::OperationResult{
                core::OperationCode::Ok, 1, 0, "cold"};
        },
        [](const core::TimeseriesBatch&) {
            grpc_core::IngestPipelineResult result;
            result.window_result = {
                core::OperationCode::Ok, 1, 0, "hot"};
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
    assert(completed.window_result.code == core::OperationCode::Ok);
    assert(elapsed_ms.count() < 180);

    std::cout << "ingest task executor test passed\n";
    return 0;
}
