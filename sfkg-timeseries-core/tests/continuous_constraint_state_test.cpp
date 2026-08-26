#include <cassert>
#include <iostream>

#include "sfkg/timeseries/core/alignment_service.hpp"
#include "sfkg/timeseries/core/constraint_check_engine.hpp"
#include "sfkg/timeseries/core/grpc/constraint_result_receiver_client.hpp"
#include "sfkg/timeseries/core/grpc/timeseries_core_grpc_service.hpp"
#include "sfkg/timeseries/core/history_query_service.hpp"
#include "sfkg/timeseries/core/ingest_service.hpp"
#include "sfkg/timeseries/core/internal/taos_client.hpp"
#include "sfkg/timeseries/core/runtime_config_registry.hpp"
#include "sfkg/timeseries/core/statistics_service.hpp"
#include "sfkg/timeseries/core/storage_service.hpp"
#include "sfkg/timeseries/core/window_service.hpp"

namespace core = sfkg::timeseries::core;
namespace core_grpc = sfkg::timeseries::core::grpc;
namespace proto = sfkg::timeseries::core::v1;

namespace {

proto::IngestDataResponse ingest(
    core_grpc::TimeseriesCoreGrpcService& service,
    const std::string& sequence_id,
    core::Timestamp time,
    double value) {
    proto::IngestDataRequest request;
    request.set_project_id("project-a");
    auto* point = request.add_points();
    point->set_sequence_id(sequence_id);
    point->set_time(time);
    point->mutable_value()->set_double_value(value);
    proto::IngestDataResponse response;
    const auto status = service.ingestData(nullptr, &request, &response);
    assert(status.ok());
    return response;
}

}  // namespace

int main() {
    core::RuntimeConfigRegistry registry;
    core::RuntimeInstanceConfig temperature;
    temperature.sequence_id = "temperature";
    temperature.data_source_id = "source";
    temperature.external_sequence_id = "temperature";
    temperature.category_id = "temperature";
    temperature.data_type = "double";
    temperature.series_kind = core::SeriesKind::Continuous;
    core::RuntimeInstanceConfig pressure = temperature;
    pressure.sequence_id = "pressure";
    pressure.external_sequence_id = "pressure";
    pressure.category_id = "pressure";
    core::RuntimeConfigSnapshot<core::RuntimeInstanceConfig>
        instance_snapshot;
    instance_snapshot.project_id = "project-a";
    instance_snapshot.items = {temperature, pressure};
    assert(registry.replaceInstanceConfigs(instance_snapshot).code ==
        core::OperationCode::Ok);

    core::ConstraintRule rule;
    rule.constraint_id = "temperature-upper";
    rule.project_id = "project-a";
    rule.variable_mapping = {{"x", "temperature"}};
    rule.upper_bound = 10.0;
    rule.terms = {{"x", 1.0, 0}};
    core::RuntimeConfigSnapshot<core::RuntimeConstraintConfig>
        constraint_snapshot;
    constraint_snapshot.project_id = "project-a";
    constraint_snapshot.items = {{rule, true, "project-a"}};
    assert(registry.replaceConstraints(constraint_snapshot).code ==
        core::OperationCode::Ok);

    core::IngestService ingest_service(registry);
    core::internal::TaosClient taos;
    core::StorageService storage(taos);
    core::WindowService window;
    assert(window.configureWindowSize("project-a", 1'000).code ==
           core::OperationCode::Ok);
    core::AlignmentService alignment(registry);
    core::StatisticsService statistics;
    core::ConstraintCheckEngine constraints;
    core::HistoryQueryService history(registry, taos);
    // An empty address makes delivery fail after enqueue, but enqueue itself
    // remains observable and does not require opening a test socket.
    core_grpc::ConstraintResultReceiverClient receiver("");
    core_grpc::TimeseriesCoreGrpcService service(
        ingest_service,
        storage,
        window,
        alignment,
        statistics,
        constraints,
        history,
        registry,
        receiver);

    auto response = ingest(service, "temperature", 0, 20.0);
    assert(response.constraint_notification_result().code() ==
           proto::OPERATION_CODE_OK);
    assert(response.constraint_notification_result().message() ==
           "constraint result notification queued");

    // The second ingest touches an unrelated sequence. The previous
    // violation is still in the current window and must be reported again.
    response = ingest(service, "pressure", 1, 1.0);
    assert(response.constraint_notification_result().code() ==
           proto::OPERATION_CODE_OK);
    assert(response.constraint_notification_result().message() ==
           "constraint result notification queued");

    // Correcting the violating timestamp clears the cached window state.
    response = ingest(service, "temperature", 0, 5.0);
    assert(response.constraint_notification_result().message() ==
           "no constraint violations; notification skipped");
    response = ingest(service, "pressure", 2, 2.0);
    assert(response.constraint_notification_result().message().find(
               "notification skipped") != std::string::npos);

    std::cout << "continuous_constraint_state_test passed\n";
    return 0;
}
