#include <cassert>
#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "sfkg/timeseries/core/grpc/constraint_result_receiver_client.hpp"

namespace core = sfkg::timeseries::core;
namespace receiver_pb = ::sfkg::timeseries::core::v1;

class Receiver final
    : public receiver_pb::TimeseriesConstraintResultReceiverService::Service {
public:
    ::grpc::Status ReceiveConstraintResult(
        ::grpc::ServerContext*,
        const receiver_pb::ConstraintResultMessage* request,
        receiver_pb::SyncResponse* response) override {
        check_time_ms = request->check_time_ms();
        violated_constraint_ids.assign(
            request->violated_constraint_ids().begin(),
            request->violated_constraint_ids().end());
        sequence_ids.assign(
            request->sequence_ids().begin(),
            request->sequence_ids().end());
        response->set_success(should_accept);
        response->set_message(should_accept ? "accepted" : "rejected");
        return ::grpc::Status::OK;
    }

    bool should_accept{true};
    core::Timestamp check_time_ms{};
    std::vector<std::string> violated_constraint_ids;
    std::vector<std::string> sequence_ids;
};

int main() {
    Receiver receiver;
    ::grpc::ServerBuilder builder;
    int selected_port = 0;
    builder.AddListeningPort(
        "127.0.0.1:0",
        ::grpc::InsecureServerCredentials(),
        &selected_port);
    builder.RegisterService(&receiver);
    auto server = builder.BuildAndStart();
    assert(server);

    core::grpc::ConstraintResultReceiverClient client(
        "127.0.0.1:" + std::to_string(selected_port));
    const auto accepted = client.receiveConstraintResult(
        123456,
        {"constraint-a", "constraint-b"},
        {"temperature-1", "humidity-1"});
    assert(accepted.code == core::OperationCode::Ok);
    assert(receiver.check_time_ms == 123456);
    assert(receiver.violated_constraint_ids.size() == 2);
    assert(receiver.violated_constraint_ids[0] == "constraint-a");
    assert(receiver.violated_constraint_ids[1] == "constraint-b");
    assert(receiver.sequence_ids.size() == 2);

    receiver.should_accept = false;
    const auto rejected = client.receiveConstraintResult(
        123457, {"constraint-c"}, {"temperature-1"});
    assert(rejected.code == core::OperationCode::InternalError);

    server->Shutdown();
    return 0;
}
