#include "sfkg/timeseries/core/grpc/ingest_task_executor.hpp"

#include <algorithm>
#include <cstdlib>
#include <exception>
#include <limits>
#include <string>
#include <utility>

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core::grpc {
namespace {

std::size_t configuredSize(
    const char* name,
    std::size_t fallback,
    std::size_t maximum) {
    const char* text = std::getenv(name);
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
    return value == 0 || value > maximum ? fallback : value;
}

OperationResult unavailable(std::size_t failed_count, std::string message) {
    return internal::makeOperationResult(
        OperationCode::Unavailable, 0, failed_count, std::move(message));
}

OperationResult workerFailure(std::size_t failed_count, std::string message) {
    return internal::makeOperationResult(
        OperationCode::InternalError, 0, failed_count, std::move(message));
}

}  // namespace

struct IngestTaskExecutor::Task {
    std::shared_ptr<const IngestResult> resolved;
    ColdWriteFunction cold_write;
    HotUpdateFunction hot_update;
    std::promise<IngestPipelineResult> completion;
    std::mutex result_mutex;
    IngestPipelineResult result;
    std::size_t remaining_lanes{2};
    IngestTaskExecutor* owner{};
};

IngestTaskExecutor::IngestTaskExecutor()
    : queue_capacity_(configuredSize(
          "SFKG_INGEST_QUEUE_CAPACITY", 128, 100000)) {
    const auto cold_worker_count = configuredSize(
        "SFKG_INGEST_COLD_WORKERS", 4, 64);
    const auto hot_worker_count = configuredSize(
        "SFKG_INGEST_HOT_WORKERS", 1, 64);
    cold_workers_.reserve(cold_worker_count);
    hot_workers_.reserve(hot_worker_count);
    for (std::size_t index = 0; index < cold_worker_count; ++index) {
        cold_workers_.emplace_back([this] { workerLoop(true); });
    }
    for (std::size_t index = 0; index < hot_worker_count; ++index) {
        hot_workers_.emplace_back([this] { workerLoop(false); });
    }
}

IngestTaskExecutor::~IngestTaskExecutor() {
    {
        std::lock_guard lock(queue_mutex_);
        stopping_ = true;
    }
    queue_condition_.notify_all();
    for (auto& worker : cold_workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    for (auto& worker : hot_workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

IngestTaskSubmission IngestTaskExecutor::trySubmit(
    std::shared_ptr<const IngestResult> resolved,
    ColdWriteFunction cold_write,
    HotUpdateFunction hot_update) {
    if (!resolved || !cold_write || !hot_update) {
        return {
            false,
            internal::invalidArgument("ingest task submission is incomplete"),
            {}};
    }

    const auto failed_count = resolved->resolved_data.points.size();
    std::unique_lock lock(queue_mutex_);
    if (stopping_) {
        return {
            false,
            unavailable(failed_count, "ingest executor is shutting down"),
            {}};
    }
    if (pending_tasks_ >= queue_capacity_) {
        return {
            false,
            unavailable(
                failed_count,
                "ingest queue is full; retry later"),
            {}};
    }

    auto task = std::make_shared<Task>();
    task->resolved = std::move(resolved);
    task->cold_write = std::move(cold_write);
    task->hot_update = std::move(hot_update);
    task->owner = this;
    auto completion = task->completion.get_future();
    ++pending_tasks_;
    // Both queue insertions happen while the admission lock is held. Since
    // pending_tasks_ counts active as well as queued tasks, each lane has
    // enough room for every admitted task, and no half-admitted pair exists.
    cold_queue_.push_back(task);
    hot_queue_.push_back(std::move(task));
    lock.unlock();
    queue_condition_.notify_all();
    return {
        true,
        internal::ok(0, "ingest task admitted"),
        std::move(completion)};
}

void IngestTaskExecutor::workerLoop(bool cold_lane) {
    while (true) {
        std::shared_ptr<Task> task;
        {
            std::unique_lock lock(queue_mutex_);
            queue_condition_.wait(lock, [this, cold_lane] {
                const auto& queue = cold_lane ? cold_queue_ : hot_queue_;
                return stopping_ || !queue.empty();
            });
            auto& queue = cold_lane ? cold_queue_ : hot_queue_;
            if (queue.empty()) {
                if (stopping_) {
                    return;
                }
                continue;
            }
            task = std::move(queue.front());
            queue.pop_front();
        }

        if (cold_lane) {
            OperationResult result;
            try {
                result = task->cold_write(task->resolved->resolved_data);
            } catch (const std::exception& exception) {
                result = workerFailure(
                    task->resolved->resolved_data.points.size(),
                    std::string("cold ingest worker failed: ") +
                        exception.what());
            } catch (...) {
                result = workerFailure(
                    task->resolved->resolved_data.points.size(),
                    "cold ingest worker failed: unknown exception");
            }
            completeCold(task, std::move(result));
        } else {
            IngestPipelineResult result;
            try {
                result = task->hot_update(task->resolved->resolved_data);
            } catch (const std::exception& exception) {
                const auto message = std::string("hot ingest worker failed: ") +
                    exception.what();
                result.window_result = workerFailure(
                    task->resolved->resolved_data.points.size(), message);
                result.derived_result = workerFailure(
                    task->resolved->resolved_data.points.size(), message);
                result.constraint_notification_result = workerFailure(
                    task->resolved->resolved_data.points.size(), message);
            } catch (...) {
                const std::string message =
                    "hot ingest worker failed: unknown exception";
                result.window_result = workerFailure(
                    task->resolved->resolved_data.points.size(), message);
                result.derived_result = workerFailure(
                    task->resolved->resolved_data.points.size(), message);
                result.constraint_notification_result = workerFailure(
                    task->resolved->resolved_data.points.size(), message);
            }
            completeHot(task, std::move(result));
        }
    }
}

void IngestTaskExecutor::completeCold(
    const std::shared_ptr<Task>& task,
    OperationResult result) {
    {
        std::lock_guard lock(task->result_mutex);
        task->result.storage_result = std::move(result);
    }
    completeTask(task);
}

void IngestTaskExecutor::completeHot(
    const std::shared_ptr<Task>& task,
    IngestPipelineResult result) {
    {
        std::lock_guard lock(task->result_mutex);
        task->result.window_result = std::move(result.window_result);
        task->result.derived_result = std::move(result.derived_result);
        task->result.constraint_notification_result =
            std::move(result.constraint_notification_result);
    }
    completeTask(task);
}

void IngestTaskExecutor::completeTask(const std::shared_ptr<Task>& task) {
    std::optional<IngestPipelineResult> completed;
    {
        std::lock_guard lock(task->result_mutex);
        if (--task->remaining_lanes == 0) {
            completed = std::move(task->result);
        }
    }
    if (!completed) {
        return;
    }

    task->completion.set_value(std::move(*completed));
    {
        std::lock_guard lock(queue_mutex_);
        if (pending_tasks_ != 0) {
            --pending_tasks_;
        }
    }
}

}  // namespace sfkg::timeseries::core::grpc
