#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "timeseries_core.grpc.pb.h"

namespace smoke {

namespace pb = sfkg::timeseries::core::v1;

namespace {

bool check(
    const char* name,
    const ::grpc::Status& status,
    const pb::OperationResult& operation,
    pb::OperationCode expected) {
    const bool passed = status.ok() && operation.code() == expected;
    std::cout << (passed ? "[PASS] " : "[FAIL] ") << name
              << " grpc=" << status.error_code()
              << " operation=" << pb::OperationCode_Name(operation.code())
              << " message=\"" << operation.message() << "\"\n";
    return passed;
}

void addInstances(pb::SyncInstanceConfigsRequest* request) {
    auto* temperature = request->add_items();
    temperature->set_sequence_id("temperature-1");
    temperature->set_data_source_id("source-a");
    temperature->set_external_sequence_id("temp");
    temperature->set_category_id("temperature");
    temperature->set_data_type("continuous");

    auto* pressure = request->add_items();
    pressure->set_sequence_id("pressure-1");
    pressure->set_data_source_id("source-a");
    pressure->set_external_sequence_id("pressure");
    pressure->set_category_id("pressure");
    pressure->set_data_type("continuous");
}

void addConstraint(pb::SyncConstraintsRequest* request) {
    auto* item = request->add_items();
    item->set_enabled(true);
    auto* rule = item->mutable_rule();
    rule->set_constraint_id("temperature-range");
    (*rule->mutable_variable_mapping())["x"] = "temperature-1";
    rule->set_lower_bound(-40.0);
    rule->set_upper_bound(120.0);
    auto* term = rule->add_terms();
    term->set_variable("x");
    term->set_coefficient(1.0);
    term->set_sample_offset(0);
}

void addWindow(pb::WindowData* data) {
    data->set_window_start_time(1'000);
    data->set_window_end_time(2'000);

    auto* temperature = data->add_sequences();
    temperature->set_sequence_id("temperature-1");
    auto* temperature_point = temperature->add_points();
    temperature_point->set_time(1'000);
    temperature_point->mutable_value()->set_double_value(25.0);

    auto* pressure = data->add_sequences();
    pressure->set_sequence_id("pressure-1");
    auto* pressure_point = pressure->add_points();
    pressure_point->set_time(1'000);
    pressure_point->mutable_value()->set_double_value(101.3);
}

void addAlignmentConfig(pb::AlignmentConfig* config) {
    config->set_bucket_interval(1'000);
    auto* independent = config->add_sequences();
    independent->set_sequence_id("temperature-1");
    independent->set_role(pb::VARIABLE_ROLE_INDEPENDENT);
    independent->set_aggregation(pb::BUCKET_AGGREGATION_AVERAGE);
    independent->set_fill_method(pb::GAP_FILL_METHOD_LINEAR);

    auto* dependent = config->add_sequences();
    dependent->set_sequence_id("pressure-1");
    dependent->set_role(pb::VARIABLE_ROLE_DEPENDENT);
    dependent->set_aggregation(pb::BUCKET_AGGREGATION_AVERAGE);
    dependent->set_fill_method(pb::GAP_FILL_METHOD_LINEAR);
}

void addRawPoint(
    pb::TimeseriesBatch* batch,
    const std::string& sequence_id,
    std::int64_t time,
    double value) {
    auto* point = batch->add_points();
    point->set_sequence_id(sequence_id);
    point->set_time(time);
    point->mutable_value()->set_double_value(value);
}

}  // namespace

int run(int argc, char* argv[]) {
    std::string address = "localhost:50051";
    if (const char* configured = std::getenv("SFKG_TIMESERIES_CORE_ADDRESS")) {
        address = configured;
    }
    if (argc > 1) {
        address = argv[1];
    }

    auto channel = ::grpc::CreateChannel(
        address, ::grpc::InsecureChannelCredentials());
    auto stub = pb::TimeseriesCoreService::NewStub(channel);
    bool passed = true;

    {
        pb::SyncInstanceConfigsRequest request;
        pb::SyncConfigResponse response;
        ::grpc::ClientContext context;
        addInstances(&request);
        const auto status = stub->syncInstanceConfigs(
            &context, request, &response);
        passed &= check(
            "syncInstanceConfigs", status, response.operation(),
            pb::OPERATION_CODE_OK);
    }

    {
        pb::SyncConstraintsRequest request;
        pb::SyncConfigResponse response;
        ::grpc::ClientContext context;
        addConstraint(&request);
        const auto status = stub->syncConstraints(
            &context, request, &response);
        passed &= check(
            "syncConstraints", status, response.operation(),
            pb::OPERATION_CODE_OK);
    }

    {
        pb::SyncRelationsRequest request;
        pb::SyncConfigResponse response;
        ::grpc::ClientContext context;
        auto* item = request.add_items();
        item->set_relation_id("temperature-to-pressure");
        auto* temperature = item->add_sources();
        temperature->set_source_sequence_id("temperature-1");
        temperature->set_weight(1.0);
        temperature->set_fixed_lag(2);
        item->set_target_sequence_id("pressure-1");
        item->set_relation_type("correlation");
        item->set_confidence(0.8);
        item->set_enabled(true);
        const auto status = stub->syncRelations(
            &context, request, &response);
        passed &= check(
            "syncRelations", status, response.operation(),
            pb::OPERATION_CODE_OK);
    }

    {
        pb::IngestDataRequest request;
        pb::IngestDataResponse response;
        ::grpc::ClientContext context;
        request.set_window_size(60'000);
        request.set_return_resolved_data(true);

        auto* double_point = request.add_points();
        double_point->set_sequence_id("temperature-1");
        double_point->set_time(1'000);
        double_point->mutable_value()->set_double_value(25.0);

        auto* second_double_point = request.add_points();
        second_double_point->set_sequence_id("temperature-1");
        second_double_point->set_time(1'001);
        second_double_point->mutable_value()->set_double_value(25.5);

        const auto status = stub->ingestData(&context, request, &response);
        passed &= check(
            "ingestData", status, response.operation(),
            pb::OPERATION_CODE_OK);
    }

    {
        pb::IngestRequest request;
        pb::IngestResponse response;
        ::grpc::ClientContext context;
        auto* point = request.add_points();
        point->set_data_source_id("source-a");
        point->set_external_sequence_id("temp");
        point->set_time(1'000);
        point->mutable_value()->set_double_value(25.0);
        const auto status = stub->ingestAndResolveData(
            &context, request, &response);
        passed &= check(
            "ingestAndResolveData", status, response.operation(),
            pb::OPERATION_CODE_OK);
    }

    {
        pb::WriteRawDataRequest request;
        pb::WriteRawDataResponse response;
        ::grpc::ClientContext context;
        addRawPoint(
            request.mutable_data(), "temperature-1", 1'700'000'000'000LL, 25.0);
        const auto status = stub->writeRawData(
            &context, request, &response);
        passed &= check(
            "writeRawData", status, response.operation(),
            pb::OPERATION_CODE_OK);
    }

    {
        pb::BuildTimeWindowRequest request;
        pb::BuildTimeWindowResponse response;
        ::grpc::ClientContext context;
        addRawPoint(request.mutable_data(), "temperature-1", 1'000, 25.0);
        request.set_window_size(60'000);
        const auto status = stub->buildTimeWindow(
            &context, request, &response);
        passed &= check(
            "buildTimeWindow", status, response.operation(),
            pb::OPERATION_CODE_OK);
    }

    {
        pb::QueryWindowDataRequest request;
        pb::QueryWindowDataResponse response;
        ::grpc::ClientContext context;
        request.add_sequence_ids("temperature-1");
        const auto status = stub->queryWindowData(
            &context, request, &response);
        passed &= check(
            "queryWindowData", status, response.operation(),
            pb::OPERATION_CODE_OK);
    }

    {
        pb::AlignWindowDataRequest request;
        pb::AlignWindowDataResponse response;
        ::grpc::ClientContext context;
        addWindow(request.mutable_data());
        addAlignmentConfig(request.mutable_config());
        const auto status = stub->alignWindowData(
            &context, request, &response);
        passed &= check(
            "alignWindowData", status, response.operation(),
            pb::OPERATION_CODE_OK);
    }

    {
        pb::ComputeStatisticsRequest request;
        pb::ComputeStatisticsResponse response;
        ::grpc::ClientContext context;
        addWindow(request.mutable_window_data());
        const auto status = stub->computeBasicStatistics(
            &context, request, &response);
        passed &= check(
            "computeBasicStatistics", status, response.operation(),
            pb::OPERATION_CODE_OK);
    }

    {
        pb::CheckConstraintsRequest request;
        pb::CheckConstraintsResponse response;
        ::grpc::ClientContext context;
        request.add_constraint_ids("temperature-range");
        addWindow(request.mutable_window_data());
        const auto status = stub->checkConstraints(
            &context, request, &response);
        passed &= check(
            "checkConstraints", status, response.operation(),
            pb::OPERATION_CODE_OK);
    }

    {
        pb::QueryHistoryDataRequest request;
        pb::QueryHistoryDataResponse response;
        ::grpc::ClientContext context;
        request.add_sequence_ids("temperature-1");
        request.set_start_time(1'699'999'999'000LL);
        request.set_end_time(1'700'000'001'000LL);
        const auto status = stub->queryHistoryData(
            &context, request, &response);
        passed &= check(
            "queryHistoryData", status, response.operation(),
            pb::OPERATION_CODE_OK);
    }

    {
        pb::IngestRequest request;
        pb::IngestResponse response;
        ::grpc::ClientContext context;
        const auto status = stub->ingestAndResolveData(
            &context, request, &response);
        passed &= check(
            "invalid empty ingest", status, response.operation(),
            pb::OPERATION_CODE_INVALID_ARGUMENT);
    }

    std::cout << (passed ? "smoke test passed\n" : "smoke test failed\n");
    return passed ? 0 : 1;
}

}  // namespace smoke

int main(int argc, char* argv[]) {
    return smoke::run(argc, argv);
}
