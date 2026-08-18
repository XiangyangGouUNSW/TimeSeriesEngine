#pragma once

#include <cstddef>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core::grpc {

// Results produced by the two sides of one logical IngestData task. The
// executor does not decide the business meaning of these results; the gRPC
// layer still combines them into the existing response format.
struct IngestPipelineResult {
    OperationResult storage_result;
    OperationResult window_result;
    OperationResult derived_result;
    OperationResult constraint_notification_result;
};

struct IngestTaskSubmission {
    bool accepted{false};
    OperationResult admission;
    // The hot pipeline completes independently so the gRPC layer can return
    // without waiting for the cold TDengine lanes. completion remains useful
    // for tests and orderly shutdown diagnostics and includes both lanes.
    std::future<IngestPipelineResult> hot_completion;
    std::future<IngestPipelineResult> completion;
};

// A bounded pair of worker queues. One task is admitted to both lanes before
// either lane starts, so a full lane rejects the whole ingest request instead
// of creating an accidental cold-only or hot-only write. The hot completion
// can be observed independently; the cold lane continues in the background.
class IngestTaskExecutor final {
public:
    using ColdWriteFunction = std::function<OperationResult(
        std::size_t,
        const TimeseriesBatch&)>;
    using HotUpdateFunction =
        std::function<IngestPipelineResult(const TimeseriesBatch&)>;

    IngestTaskExecutor();
    ~IngestTaskExecutor();

    IngestTaskExecutor(const IngestTaskExecutor&) = delete;
    IngestTaskExecutor& operator=(const IngestTaskExecutor&) = delete;

    IngestTaskSubmission trySubmit(
        std::shared_ptr<const IngestResult> resolved,
        ColdWriteFunction cold_write,
        HotUpdateFunction hot_update);

private:
    struct Task;

    void coldWorkerLoop(std::size_t worker_index);
    void hotWorkerLoop();
    std::size_t writerForSequence(
        const ProjectId& project_id,
        const SequenceId& sequence_id);
    void completeCold(
        const std::shared_ptr<Task>& task,
        OperationResult result);
    void completeHot(
        const std::shared_ptr<Task>& task,
        IngestPipelineResult result);
    void completeTask(const std::shared_ptr<Task>& task);

    std::mutex queue_mutex_;
    // A sequence is assigned once for the lifetime of this Core process.
    // This preserves same-sequence write serialization without relying on
    // the distribution quality of std::hash for a small sequence set.
    std::mutex routing_mutex_;
    std::unordered_map<std::string, std::size_t> sequence_writers_;
    std::size_t next_writer_index_{0};
    // The last accepted hot task for each sequence. A later task waits for
    // these futures before entering the hot pipeline, preserving submission
    // order for overlapping sequences while disjoint sequences still run on
    // different hot workers.
    std::unordered_map<std::string, std::shared_future<void>>
        sequence_hot_tails_;
    std::condition_variable queue_condition_;
    const std::size_t cold_worker_count_;
    const std::size_t queue_capacity_;
    std::vector<std::deque<std::shared_ptr<Task>>> cold_queues_;
    std::deque<std::shared_ptr<Task>> hot_queue_;
    std::size_t pending_tasks_{0};
    bool stopping_{false};
    std::vector<std::thread> cold_workers_;
    std::vector<std::thread> hot_workers_;
};

}  // namespace sfkg::timeseries::core::grpc
