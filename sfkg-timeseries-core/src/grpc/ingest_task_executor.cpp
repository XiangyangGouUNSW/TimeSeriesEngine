#include "sfkg/timeseries/core/grpc/ingest_task_executor.hpp"

#include <algorithm>
#include <cstdlib>
#include <exception>
#include <string>
#include <unordered_map>
#include <unordered_set>
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

bool isSuccessful(OperationCode code) {
    return code == OperationCode::Ok ||
        code == OperationCode::PartialSuccess;
}

void mergeStorageResult(
    OperationResult* aggregate,
    bool* initialized,
    const OperationResult& part) {
    if (!*initialized) {
        *aggregate = part;
        *initialized = true;
        return;
    }

    const bool aggregate_success = isSuccessful(aggregate->code);
    const bool part_success = isSuccessful(part.code);
    const bool had_partial =
        aggregate->code == OperationCode::PartialSuccess ||
        part.code == OperationCode::PartialSuccess;
    aggregate->success_count += part.success_count;
    aggregate->failed_count += part.failed_count;

    if (!aggregate_success || !part_success) {
        // A mixed result is represented consistently with the existing
        // IngestData response: some shards succeeded and some failed.
        aggregate->code = aggregate->success_count == 0
            ? aggregate->code
            : OperationCode::PartialSuccess;
    } else if (had_partial) {
        aggregate->code = OperationCode::PartialSuccess;
    }

    if (!part.message.empty() && part.message != aggregate->message) {
        if (!aggregate->message.empty()) {
            aggregate->message += "; ";
        }
        aggregate->message += part.message;
    }
}

}  // namespace

struct IngestTaskExecutor::Task {
    std::shared_ptr<const IngestResult> resolved;
    // One cold sub-batch per fixed writer shard. A sequence_id is placed in
    // exactly one sub-batch, so its writes stay on one writer queue.
    std::vector<TimeseriesBatch> cold_batches;
    ColdWriteFunction cold_write;
    HotUpdateFunction hot_update;
    std::promise<IngestPipelineResult> completion;
    std::mutex result_mutex;
    IngestPipelineResult result;
    bool storage_initialized{false};
    std::size_t remaining_lanes{1};
};

IngestTaskExecutor::IngestTaskExecutor()
    : cold_worker_count_(configuredSize(
          "SFKG_INGEST_COLD_WORKERS", 4, 64)),
      queue_capacity_(configuredSize(
          "SFKG_INGEST_QUEUE_CAPACITY", 128, 100000)),
      cold_queues_(cold_worker_count_) {
    const auto hot_worker_count = configuredSize(
        "SFKG_INGEST_HOT_WORKERS", 1, 64);
    cold_workers_.reserve(cold_worker_count_);
    hot_workers_.reserve(hot_worker_count);
    for (std::size_t index = 0; index < cold_worker_count_; ++index) {
        cold_workers_.emplace_back(
            [this, index] { coldWorkerLoop(index); });
    }
    for (std::size_t index = 0; index < hot_worker_count; ++index) {
        hot_workers_.emplace_back([this] { hotWorkerLoop(); });
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

std::size_t IngestTaskExecutor::writerForSequence(
    const SequenceId& sequence_id) {
    std::lock_guard lock(routing_mutex_);
    const auto existing = sequence_writers_.find(sequence_id);
    if (existing != sequence_writers_.end()) {
        return existing->second;
    }
    const auto writer_index = next_writer_index_;
    next_writer_index_ = (next_writer_index_ + 1) % cold_worker_count_;
    sequence_writers_.emplace(sequence_id, writer_index);
    return writer_index;
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

    auto task = std::make_shared<Task>();
    task->resolved = std::move(resolved);
    task->cold_write = std::move(cold_write);
    task->hot_update = std::move(hot_update);
    task->cold_batches.resize(cold_worker_count_);

    std::vector<SequenceId> batch_sequence_ids;
    std::unordered_map<SequenceId, std::size_t> batch_writers;
    std::unordered_set<SequenceId> seen_sequence_ids;
    batch_sequence_ids.reserve(task->resolved->resolved_data.points.size());
    batch_writers.reserve(task->resolved->resolved_data.points.size());
    seen_sequence_ids.reserve(task->resolved->resolved_data.points.size());
    for (const auto& point : task->resolved->resolved_data.points) {
        if (seen_sequence_ids.insert(point.sequence_id).second) {
            batch_sequence_ids.push_back(point.sequence_id);
        }
    }
    // Assign new sequences in a deterministic order within one batch. This
    // gives a small fixed sequence set a predictable round-robin distribution.
    std::sort(batch_sequence_ids.begin(), batch_sequence_ids.end());
    for (const auto& sequence_id : batch_sequence_ids) {
        batch_writers.emplace(
            sequence_id,
            writerForSequence(sequence_id));
    }

    std::vector<bool> has_cold_work(cold_worker_count_, false);
    for (const auto& point : task->resolved->resolved_data.points) {
        const auto shard = batch_writers.at(point.sequence_id);
        task->cold_batches[shard].points.push_back(point);
        has_cold_work[shard] = true;
    }
    task->remaining_lanes = 1;
    for (const bool has_work : has_cold_work) {
        if (has_work) {
            ++task->remaining_lanes;
        }
    }

    const auto failed_count = task->resolved->resolved_data.points.size();
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
            unavailable(failed_count, "ingest queue is full; retry later"),
            {}};
    }

    auto completion = task->completion.get_future();
    ++pending_tasks_;
    // Admission and all shard queue insertions are protected together. A
    // request is therefore either present in every required lane or rejected.
    for (std::size_t index = 0; index < cold_worker_count_; ++index) {
        if (has_cold_work[index]) {
            cold_queues_[index].push_back(task);
        }
    }
    hot_queue_.push_back(std::move(task));
    lock.unlock();
    queue_condition_.notify_all();
    return {
        true,
        internal::ok(0, "ingest task admitted"),
        std::move(completion)};
}

void IngestTaskExecutor::coldWorkerLoop(std::size_t worker_index) {
    while (true) {
        std::shared_ptr<Task> task;
        {
            std::unique_lock lock(queue_mutex_);
            queue_condition_.wait(lock, [this, worker_index] {
                return stopping_ || !cold_queues_[worker_index].empty();
            });
            auto& queue = cold_queues_[worker_index];
            if (queue.empty()) {
                if (stopping_) {
                    return;
                }
                continue;
            }
            task = std::move(queue.front());
            queue.pop_front();
        }

        OperationResult result;
        try {
            result = task->cold_write(
                worker_index,
                task->cold_batches[worker_index]);
        } catch (const std::exception& exception) {
            result = workerFailure(
                task->cold_batches[worker_index].points.size(),
                std::string("cold ingest worker failed: ") +
                    exception.what());
        } catch (...) {
            result = workerFailure(
                task->cold_batches[worker_index].points.size(),
                "cold ingest worker failed: unknown exception");
        }
        completeCold(task, std::move(result));
    }
}

void IngestTaskExecutor::hotWorkerLoop() {
    while (true) {
        std::shared_ptr<Task> task;
        {
            std::unique_lock lock(queue_mutex_);
            queue_condition_.wait(lock, [this] {
                return stopping_ || !hot_queue_.empty();
            });
            if (hot_queue_.empty()) {
                if (stopping_) {
                    return;
                }
                continue;
            }
            task = std::move(hot_queue_.front());
            hot_queue_.pop_front();
        }

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

void IngestTaskExecutor::completeCold(
    const std::shared_ptr<Task>& task,
    OperationResult result) {
    {
        std::lock_guard lock(task->result_mutex);
        mergeStorageResult(
            &task->result.storage_result,
            &task->storage_initialized,
            result);
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
