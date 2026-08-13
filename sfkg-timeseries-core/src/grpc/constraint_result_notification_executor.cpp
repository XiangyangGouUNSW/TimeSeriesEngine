#include "sfkg/timeseries/core/grpc/constraint_result_notification_executor.hpp"

#include <cstdlib>
#include <iostream>
#include <utility>

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core::grpc {
namespace {

std::size_t configuredCapacity() {
    constexpr std::size_t fallback = 1024;
    constexpr std::size_t maximum = 100000;
    const char* text = std::getenv(
        "SFKG_CONSTRAINT_NOTIFY_QUEUE_CAPACITY");
    if (text == nullptr || *text == '\0') {
        return fallback;
    }
    std::size_t value = 0;
    for (const char* cursor = text; *cursor != '\0'; ++cursor) {
        if (*cursor < '0' || *cursor > '9') {
            return fallback;
        }
        const auto digit = static_cast<std::size_t>(*cursor - '0');
        if (value > (maximum - digit) / 10) {
            return fallback;
        }
        value = value * 10 + digit;
    }
    return value == 0 ? fallback : value;
}

}  // namespace

ConstraintResultNotificationExecutor::ConstraintResultNotificationExecutor(
    ConstraintResultReceiverClient& receiver)
    : receiver_(receiver),
      capacity_(configuredCapacity()),
      worker_([this] { workerLoop(); }) {}

ConstraintResultNotificationExecutor::~ConstraintResultNotificationExecutor() {
    {
        std::lock_guard lock(mutex_);
        stopping_ = true;
    }
    condition_.notify_one();
    if (worker_.joinable()) {
        worker_.join();
    }
}

OperationResult ConstraintResultNotificationExecutor::tryEnqueue(
    Timestamp check_time_ms,
    std::vector<std::string> violated_constraint_ids,
    std::vector<SequenceId> sequence_ids) {
    if (violated_constraint_ids.empty()) {
        return internal::ok(0, "no constraint violations to notify");
    }

    std::lock_guard lock(mutex_);
    if (stopping_) {
        return internal::makeOperationResult(
            OperationCode::Unavailable,
            0,
            violated_constraint_ids.size(),
            "constraint notification executor is shutting down");
    }
    if (queue_.size() >= capacity_) {
        return internal::makeOperationResult(
            OperationCode::Unavailable,
            0,
            violated_constraint_ids.size(),
            "constraint notification queue is full; retry later");
    }
    queue_.push_back({
        check_time_ms,
        std::move(violated_constraint_ids),
        std::move(sequence_ids)});
    condition_.notify_one();
    return internal::ok(
        queue_.back().violated_constraint_ids.size(),
        "constraint result notification queued");
}

void ConstraintResultNotificationExecutor::workerLoop() {
    while (true) {
        Event event;
        {
            std::unique_lock lock(mutex_);
            condition_.wait(lock, [this] {
                return stopping_ || !queue_.empty();
            });
            if (queue_.empty()) {
                if (stopping_) {
                    return;
                }
                continue;
            }
            event = std::move(queue_.front());
            queue_.pop_front();
        }

        const auto result = receiver_.receiveConstraintResult(
            event.check_time_ms,
            event.violated_constraint_ids,
            event.sequence_ids);
        if (result.code != OperationCode::Ok &&
            result.code != OperationCode::PartialSuccess) {
            std::cerr << "constraint result notification failed after enqueue: "
                      << result.message << '\n';
        }
    }
}

}  // namespace sfkg::timeseries::core::grpc
