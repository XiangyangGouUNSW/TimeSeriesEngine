#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "sfkg/timeseries/core/alignment_service.hpp"
#include "sfkg/timeseries/core/constraint_check_engine.hpp"
#include "sfkg/timeseries/core/grpc/timeseries_core_grpc_service.hpp"
#include "sfkg/timeseries/core/history_query_service.hpp"
#include "sfkg/timeseries/core/ingest_service.hpp"
#include "sfkg/timeseries/core/internal/taos_client.hpp"
#include "sfkg/timeseries/core/runtime_config_registry.hpp"
#include "sfkg/timeseries/core/statistics_service.hpp"
#include "sfkg/timeseries/core/storage_service.hpp"
#include "sfkg/timeseries/core/window_service.hpp"

namespace core = sfkg::timeseries::core;

namespace {

const std::vector<std::string> kEtth1SequenceIds{
    "ETTh1_HUFL", "ETTh1_HULL", "ETTh1_MUFL", "ETTh1_MULL",
    "ETTh1_LUFL", "ETTh1_LULL", "ETTh1_OT"};

core::OperationResult registerEtth1Instances(
    core::RuntimeConfigRegistry* registry) {
    core::RuntimeConfigSnapshot<core::RuntimeInstanceConfig> snapshot;
    snapshot.items.reserve(kEtth1SequenceIds.size());
    for (const auto& sequence_id : kEtth1SequenceIds) {
        snapshot.items.push_back({
            sequence_id,
            "ETTh1.csv",
            sequence_id,
            "ETTh1",
            "double"});
    }
    return registry->replaceInstanceConfigs(snapshot);
}

}  // namespace

int main(int argc, char* argv[]) {
    const std::string address = argc > 1 ? argv[1] : "0.0.0.0:50052";

    core::RuntimeConfigRegistry registry;
    const auto registered = registerEtth1Instances(&registry);
    if (registered.code != core::OperationCode::Ok) {
        std::cerr << "failed to register ETTh1 instances: "
                  << registered.message << '\n';
        return 1;
    }

    core::internal::TaosClient taos_client;
    const auto schema = taos_client.ensureSchema();
    if (schema.code != core::OperationCode::Ok) {
        std::cerr << "failed to initialize TDengine schema: "
                  << schema.message << '\n';
        return 1;
    }

    core::IngestService ingest(registry);
    core::StorageService storage(taos_client);
    core::WindowService window;
    core::AlignmentService alignment(registry);
    core::StatisticsService statistics;
    core::ConstraintCheckEngine constraints;
    core::HistoryQueryService history(registry, taos_client);
    core::grpc::TimeseriesCoreGrpcService service(
        ingest,
        storage,
        window,
        alignment,
        statistics,
        constraints,
        history,
        registry);

    ::grpc::EnableDefaultHealthCheckService(true);
    ::grpc::ServerBuilder builder;
    builder.AddListeningPort(address, ::grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<::grpc::Server> server = builder.BuildAndStart();
    if (!server) {
        std::cerr << "failed to start ETTh1 temporary Core on "
                  << address << '\n';
        return 1;
    }

    std::cout << "ETTh1 temporary Core listening on " << address << '\n'
              << "registered instances: " << registered.success_count << '\n';
    server->Wait();
    return 0;
}
