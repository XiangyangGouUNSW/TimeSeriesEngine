#pragma once

#include <grpcpp/grpcpp.h>

#include "timeseries_core.grpc.pb.h"
#include "sfkg/timeseries/core/alignment_service.hpp"
#include "sfkg/timeseries/core/constraint_check_engine.hpp"
#include "sfkg/timeseries/core/grpc/constraint_result_receiver_client.hpp"
#include "sfkg/timeseries/core/history_query_service.hpp"
#include "sfkg/timeseries/core/ingest_service.hpp"
#include "sfkg/timeseries/core/runtime_config_registry.hpp"
#include "sfkg/timeseries/core/statistics_service.hpp"
#include "sfkg/timeseries/core/storage_service.hpp"
#include "sfkg/timeseries/core/window_service.hpp"

namespace sfkg::timeseries::core::grpc {

namespace pb = ::sfkg::timeseries::core::v1;

class TimeseriesCoreGrpcService final
    : public pb::TimeseriesCoreService::Service {
public:
    TimeseriesCoreGrpcService(
        IngestService& ingest_service,
        StorageService& storage_service,
        WindowService& window_service,
        AlignmentService& alignment_service,
        StatisticsService& statistics_service,
        ConstraintCheckEngine& constraint_engine,
        HistoryQueryService& history_service,
        RuntimeConfigRegistry& config_registry,
        ConstraintResultReceiverClient& constraint_result_receiver)
        : ingest_service_(ingest_service),
          storage_service_(storage_service),
          window_service_(window_service),
          alignment_service_(alignment_service),
          statistics_service_(statistics_service),
          constraint_engine_(constraint_engine),
          history_service_(history_service),
          config_registry_(config_registry),
          constraint_result_receiver_(constraint_result_receiver) {}

    ::grpc::Status syncInstanceConfigs(
        ::grpc::ServerContext* context,
        const pb::SyncInstanceConfigsRequest* request,
        pb::SyncConfigResponse* response) override;
    ::grpc::Status syncConstraints(
        ::grpc::ServerContext* context,
        const pb::SyncConstraintsRequest* request,
        pb::SyncConfigResponse* response) override;
    ::grpc::Status syncRelations(
        ::grpc::ServerContext* context,
        const pb::SyncRelationsRequest* request,
        pb::SyncConfigResponse* response) override;
    ::grpc::Status syncWindowConfig(
        ::grpc::ServerContext* context,
        const pb::SyncWindowConfigRequest* request,
        pb::SyncConfigResponse* response) override;

    ::grpc::Status ingestData(
        ::grpc::ServerContext* context,
        const pb::IngestDataRequest* request,
        pb::IngestDataResponse* response) override;
    ::grpc::Status ingestAndResolveData(
        ::grpc::ServerContext* context,
        const pb::IngestRequest* request,
        pb::IngestResponse* response) override;
    ::grpc::Status writeRawData(
        ::grpc::ServerContext* context,
        const pb::WriteRawDataRequest* request,
        pb::WriteRawDataResponse* response) override;
    ::grpc::Status buildTimeWindow(
        ::grpc::ServerContext* context,
        const pb::BuildTimeWindowRequest* request,
        pb::BuildTimeWindowResponse* response) override;
    ::grpc::Status queryWindowData(
        ::grpc::ServerContext* context,
        const pb::QueryWindowDataRequest* request,
        pb::QueryWindowDataResponse* response) override;
    ::grpc::Status alignWindowData(
        ::grpc::ServerContext* context,
        const pb::AlignWindowDataRequest* request,
        pb::AlignWindowDataResponse* response) override;
    ::grpc::Status computeBasicStatistics(
        ::grpc::ServerContext* context,
        const pb::ComputeStatisticsRequest* request,
        pb::ComputeStatisticsResponse* response) override;
    ::grpc::Status checkConstraints(
        ::grpc::ServerContext* context,
        const pb::CheckConstraintsRequest* request,
        pb::CheckConstraintsResponse* response) override;
    ::grpc::Status queryHistoryData(
        ::grpc::ServerContext* context,
        const pb::QueryHistoryDataRequest* request,
        pb::QueryHistoryDataResponse* response) override;
    ::grpc::Status queryHistoryOverview(
        ::grpc::ServerContext* context,
        const pb::QueryHistoryOverviewRequest* request,
        pb::QueryHistoryOverviewResponse* response) override;

private:
    IngestService& ingest_service_;
    StorageService& storage_service_;
    WindowService& window_service_;
    AlignmentService& alignment_service_;
    StatisticsService& statistics_service_;
    ConstraintCheckEngine& constraint_engine_;
    HistoryQueryService& history_service_;
    RuntimeConfigRegistry& config_registry_;
    ConstraintResultReceiverClient& constraint_result_receiver_;
};

}  // namespace sfkg::timeseries::core::grpc
