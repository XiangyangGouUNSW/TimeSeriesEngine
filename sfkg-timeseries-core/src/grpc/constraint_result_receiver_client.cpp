#include "sfkg/timeseries/core/grpc/constraint_result_receiver_client.hpp"

#include <array>
#include <chrono>
#include <thread>
#include <utility>

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core::grpc {

namespace {

constexpr std::array<std::chrono::seconds, 3> kRetryDelays{
    std::chrono::seconds{10},
    std::chrono::seconds{20},
    std::chrono::seconds{40}};

}  // namespace

ConstraintResultReceiverClient::ConstraintResultReceiverClient(
    std::string address)
    : address_(std::move(address)) {
    if (!address_.empty()) {
        stub_ = pb::TimeseriesConstraintResultReceiverService::NewStub(
            ::grpc::CreateChannel(
                address_, ::grpc::InsecureChannelCredentials()));
    }
}

OperationResult ConstraintResultReceiverClient::receiveConstraintResult(
    const ProjectId& project_id,
    Timestamp check_time_ms,
    const std::vector<std::string>& violated_constraint_ids,
    const std::vector<SequenceId>& sequence_ids) {
    if (violated_constraint_ids.empty()) {
        return internal::ok(0, "no constraint violations to notify");
    }
    if (!stub_) {
        return internal::invalidArgument(
            "constraint result receiver address is not configured");
    }

    pb::ConstraintResultMessage request;
    request.set_project_id(project_id);
    request.set_check_time_ms(check_time_ms);
    for (const auto& constraint_id : violated_constraint_ids) {
        request.add_violated_constraint_ids(constraint_id);
    }
    for (const auto& sequence_id : sequence_ids) {
        request.add_sequence_ids(sequence_id);
    }

    std::string last_error;
    for (std::size_t attempt = 0;; ++attempt) {
        pb::SyncResponse response;
        ::grpc::ClientContext context;
        context.set_deadline(
            std::chrono::system_clock::now() + std::chrono::seconds(1));
        const ::grpc::Status status = stub_->ReceiveConstraintResult(
            &context, request, &response);
        if (status.ok()) {
            // A valid application response means the receiver was reached.
            // An explicit business rejection is not a transport failure and
            // must not be retried.
            if (!response.success()) {
                return internal::makeOperationResult(
                    OperationCode::InternalError,
                    0,
                    violated_constraint_ids.size(),
                    "constraint result receiver rejected result: " +
                        response.message());
            }
            return internal::ok(
                violated_constraint_ids.size(),
                response.message().empty()
                    ? "constraint result notified"
                    : response.message());
        }

        last_error = status.error_message();
        if (attempt >= kRetryDelays.size()) {
            return internal::makeOperationResult(
                OperationCode::Unavailable,
                0,
                violated_constraint_ids.size(),
                "constraint result receiver RPC failed after " +
                    std::to_string(attempt + 1) + " attempts: " +
                    last_error);
        }

        std::this_thread::sleep_for(kRetryDelays[attempt]);
    }
}

OperationResult ConstraintResultReceiverClient::receiveConstraintResult(
    Timestamp check_time_ms,
    const std::vector<std::string>& violated_constraint_ids,
    const std::vector<SequenceId>& sequence_ids) {
    return receiveConstraintResult(
        "default", check_time_ms, violated_constraint_ids, sequence_ids);
}

}  // namespace sfkg::timeseries::core::grpc
