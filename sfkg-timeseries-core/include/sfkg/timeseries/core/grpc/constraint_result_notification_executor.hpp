#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "sfkg/timeseries/core/grpc/constraint_result_receiver_client.hpp"

namespace sfkg::timeseries::core::grpc {

// Decouples outbound violation delivery from the hot ingest worker. Enqueue
// is bounded and non-blocking with respect to the remote service; a dedicated
// worker owns the synchronous RPC and drains the queue during shutdown.
class ConstraintResultNotificationExecutor final {
public:
    explicit ConstraintResultNotificationExecutor(
        ConstraintResultReceiverClient& receiver);
    ~ConstraintResultNotificationExecutor();

    ConstraintResultNotificationExecutor(
        const ConstraintResultNotificationExecutor&) = delete;
    ConstraintResultNotificationExecutor& operator=(
        const ConstraintResultNotificationExecutor&) = delete;

    OperationResult tryEnqueue(
        const ProjectId& project_id,
        Timestamp check_time_ms,
        std::vector<std::string> violated_constraint_ids,
        std::vector<SequenceId> sequence_ids);
    OperationResult tryEnqueue(
        Timestamp check_time_ms,
        std::vector<std::string> violated_constraint_ids,
        std::vector<SequenceId> sequence_ids);

private:
    struct Event {
        ProjectId project_id;
        Timestamp check_time_ms{};
        std::vector<std::string> violated_constraint_ids;
        std::vector<SequenceId> sequence_ids;
    };

    void workerLoop();

    ConstraintResultReceiverClient& receiver_;
    const std::size_t capacity_;
    std::mutex mutex_;
    std::condition_variable condition_;
    std::deque<Event> queue_;
    bool stopping_{false};
    std::thread worker_;
};

}  // namespace sfkg::timeseries::core::grpc
