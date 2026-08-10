#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "sfkg/timeseries/core/alignment_service.hpp"
#include "sfkg/timeseries/core/constraint_check_engine.hpp"
#include "sfkg/timeseries/core/grpc/timeseries_core_grpc_service.hpp"
#include "sfkg/timeseries/core/history_query_service.hpp"
#include "sfkg/timeseries/core/internal/taos_client.hpp"
#include "sfkg/timeseries/core/ingest_service.hpp"
#include "sfkg/timeseries/core/runtime_config_registry.hpp"
#include "sfkg/timeseries/core/statistics_service.hpp"
#include "sfkg/timeseries/core/storage_service.hpp"
#include "sfkg/timeseries/core/window_service.hpp"

namespace core = sfkg::timeseries::core;

int main(int argc, char* argv[]) {
    std::string address = "0.0.0.0:50051";
    if (const char* configured = std::getenv("SFKG_TIMESERIES_CORE_ADDRESS")) {
        address = configured;
    }
    if (argc > 1) {
        address = argv[1];
    }
    std::string constraint_result_receiver_address =
        "222.29.156.142:9105";
    if (const char* configured = std::getenv(
            "SFKG_CONSTRAINT_RESULT_RECEIVER_ADDRESS")) {
        constraint_result_receiver_address = configured;
    }

    core::RuntimeConfigRegistry registry;
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
    core::grpc::ConstraintResultReceiverClient constraint_result_receiver(
        constraint_result_receiver_address);
    core::grpc::TimeseriesCoreGrpcService service(
        ingest,
        storage,
        window,
        alignment,
        statistics,
        constraints,
        history,
        registry,
        constraint_result_receiver);

    ::grpc::EnableDefaultHealthCheckService(true);
    ::grpc::ServerBuilder builder;
    builder.AddListeningPort(address, ::grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<::grpc::Server> server = builder.BuildAndStart();
    if (!server) {
        std::cerr << "failed to start sfkg-timeseries-core on "
                  << address << '\n';
        return 1;
    }

    std::cout << "sfkg-timeseries-core listening on " << address << '\n';
    server->Wait();
    return 0;
}
