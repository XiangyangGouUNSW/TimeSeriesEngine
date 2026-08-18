#pragma once

#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "timeseries_core.grpc.pb.h"
#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core::grpc {

namespace pb = ::sfkg::timeseries::core::v1;

// Outbound client used by Core to report violations after an ingest check.
// It owns no business state; the unified service remains the owner of event
// persistence and task lifecycle.
class ConstraintResultReceiverClient final {
public:
    explicit ConstraintResultReceiverClient(std::string address);

    OperationResult receiveConstraintResult(
        const ProjectId& project_id,
        Timestamp check_time_ms,
        const std::vector<std::string>& violated_constraint_ids,
        const std::vector<SequenceId>& sequence_ids);
    OperationResult receiveConstraintResult(
        Timestamp check_time_ms,
        const std::vector<std::string>& violated_constraint_ids,
        const std::vector<SequenceId>& sequence_ids);

private:
    std::string address_;
    std::unique_ptr<pb::TimeseriesConstraintResultReceiverService::Stub>
        stub_;
};

}  // namespace sfkg::timeseries::core::grpc
