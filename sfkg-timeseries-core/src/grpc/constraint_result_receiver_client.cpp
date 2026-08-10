#include "sfkg/timeseries/core/grpc/constraint_result_receiver_client.hpp"

#include <chrono>
#include <utility>

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core::grpc {

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
    request.set_check_time_ms(check_time_ms);
    for (const auto& constraint_id : violated_constraint_ids) {
        request.add_violated_constraint_ids(constraint_id);
    }
    for (const auto& sequence_id : sequence_ids) {
        request.add_sequence_ids(sequence_id);
    }

    pb::SyncResponse response;
    ::grpc::ClientContext context;
    context.set_deadline(
        std::chrono::system_clock::now() + std::chrono::seconds(1));
    const ::grpc::Status status = stub_->ReceiveConstraintResult(
        &context, request, &response);
    if (!status.ok()) {
        return internal::makeOperationResult(
            OperationCode::Unavailable,
            0,
            violated_constraint_ids.size(),
            "constraint result receiver RPC failed: " +
                status.error_message());
    }
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

}  // namespace sfkg::timeseries::core::grpc
