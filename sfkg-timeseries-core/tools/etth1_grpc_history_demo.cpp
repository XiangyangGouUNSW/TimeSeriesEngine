#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "timeseries_core.grpc.pb.h"

namespace {

namespace api = ::sfkg::timeseries::core::v1;

constexpr std::int64_t kFirstTime = 1'467'331'200'000LL;
constexpr std::int64_t kLastTime = 1'530'039'600'000LL;
constexpr std::int64_t kHour = 3'600'000LL;

const std::vector<std::string> kSequenceIds{
    "ETTh1_HUFL", "ETTh1_HULL", "ETTh1_MUFL", "ETTh1_MULL",
    "ETTh1_LUFL", "ETTh1_LULL", "ETTh1_OT"};

const char* operationName(api::OperationCode code) {
    switch (code) {
    case api::OPERATION_CODE_OK:
        return "OK";
    case api::OPERATION_CODE_PARTIAL_SUCCESS:
        return "PARTIAL_SUCCESS";
    case api::OPERATION_CODE_INVALID_ARGUMENT:
        return "INVALID_ARGUMENT";
    case api::OPERATION_CODE_NOT_FOUND:
        return "NOT_FOUND";
    case api::OPERATION_CODE_FAILED_PRECONDITION:
        return "FAILED_PRECONDITION";
    case api::OPERATION_CODE_UNAVAILABLE:
        return "UNAVAILABLE";
    case api::OPERATION_CODE_INTERNAL_ERROR:
        return "INTERNAL_ERROR";
    case api::OPERATION_CODE_NOT_IMPLEMENTED:
        return "NOT_IMPLEMENTED";
    }
    return "UNKNOWN";
}

void printOperation(const char* name, const api::OperationResult& operation) {
    std::cout << name << ": " << operationName(operation.code())
              << " success=" << operation.success_count()
              << " failed=" << operation.failed_count()
              << " message=" << operation.message() << '\n';
}

void printValue(const api::TimeseriesValue& value) {
    switch (value.kind_case()) {
    case api::TimeseriesValue::kDoubleValue:
        std::cout << value.double_value();
        break;
    case api::TimeseriesValue::kInt64Value:
        std::cout << value.int64_value();
        break;
    case api::TimeseriesValue::kBoolValue:
        std::cout << (value.bool_value() ? "true" : "false");
        break;
    case api::TimeseriesValue::kStringValue:
        std::cout << value.string_value();
        break;
    case api::TimeseriesValue::KIND_NOT_SET:
        std::cout << "<unset>";
        break;
    }
}

void printPoints(const api::TimeseriesBatch& data) {
    constexpr int kPrintLimit = 30;
    std::cout << "returned points: " << data.points_size() << '\n';
    const int count = data.points_size() < kPrintLimit
        ? data.points_size()
        : kPrintLimit;
    for (int index = 0; index < count; ++index) {
        const auto& point = data.points(index);
        std::cout << "  time=" << point.time()
                  << " sequence_id=" << point.sequence_id() << " value=";
        printValue(point.value());
        std::cout << '\n';
    }
    if (data.points_size() > kPrintLimit) {
        std::cout << "  ... only the first " << kPrintLimit
                  << " points are shown\n";
    }
}

void setSequenceIds(::google::protobuf::RepeatedPtrField<std::string>* target) {
    for (const auto& sequence_id : kSequenceIds) {
        target->Add()->assign(sequence_id);
    }
}

bool syncSequences(
    api::TimeseriesCoreService::Stub* stub) {
    api::SyncInstanceConfigsRequest request;
    for (const auto& sequence_id : kSequenceIds) {
        auto* item = request.add_items();
        item->set_sequence_id(sequence_id);
        item->set_data_source_id("ETTh1.csv");
        item->set_external_sequence_id(sequence_id);
        item->set_category_id("ETTh1");
        item->set_data_type("double");
    }
    api::SyncConfigResponse response;
    ::grpc::ClientContext context;
    const auto status = stub->syncInstanceConfigs(
        &context, request, &response);
    if (!status.ok()) {
        std::cout << "syncInstanceConfigs grpc error: "
                  << status.error_message() << '\n';
        return false;
    }
    printOperation("syncInstanceConfigs", response.operation());
    return response.operation().code() == api::OPERATION_CODE_OK;
}

bool queryOverview(
    api::TimeseriesCoreService::Stub* stub) {
    api::QueryHistoryOverviewRequest request;
    setSequenceIds(request.mutable_sequence_ids());
    request.set_start_time(kFirstTime);
    request.set_end_time(kLastTime + 1);
    api::QueryHistoryOverviewResponse response;
    ::grpc::ClientContext context;
    const auto status = stub->queryHistoryOverview(
        &context, request, &response);
    if (!status.ok()) {
        std::cout << "queryHistoryOverview grpc error: "
                  << status.error_message() << '\n';
        return false;
    }
    printOperation("queryHistoryOverview", response.operation());
    if (response.operation().code() != api::OPERATION_CODE_OK) {
        return false;
    }
    const auto& overview = response.overview();
    std::cout << "overview: sequences=" << overview.sequence_count()
              << " points=" << overview.total_point_count() << '\n';
    std::cout << "column order:";
    for (const auto& name : overview.column_names()) {
        std::cout << ' ' << name;
    }
    std::cout << '\n';
    for (const auto& series : overview.series()) {
        std::cout << "  " << series.sequence_id()
                  << ": " << series.point_count() << " points, last_time="
                  << (series.has_last_time() ? series.last_time() : 0) << '\n';
    }
    return true;
}

bool queryData(
    api::TimeseriesCoreService::Stub* stub,
    std::int64_t start,
    std::int64_t end) {
    if (start > end) {
        std::cout << "start_time must not be after end_time\n";
        return false;
    }
    api::QueryHistoryDataRequest request;
    setSequenceIds(request.mutable_sequence_ids());
    request.set_start_time(start);
    request.set_end_time(end);
    api::QueryHistoryDataResponse response;
    ::grpc::ClientContext context;
    const auto status = stub->queryHistoryData(
        &context, request, &response);
    if (!status.ok()) {
        std::cout << "queryHistoryData grpc error: "
                  << status.error_message() << '\n';
        return false;
    }
    printOperation("queryHistoryData", response.operation());
    if (response.operation().code() != api::OPERATION_CODE_OK) {
        return false;
    }
    printPoints(response.data());
    return true;
}

}  // namespace

int main(int argc, char* argv[]) {
    const std::string address = argc > 1 ? argv[1] : "127.0.0.1:50052";
    std::cout << "connecting to Core at " << address << '\n';
    auto channel = ::grpc::CreateChannel(
        address, ::grpc::InsecureChannelCredentials());
    if (!channel->WaitForConnected(
            std::chrono::system_clock::now() + std::chrono::seconds(5))) {
        std::cerr << "Core is not reachable at " << address << '\n';
        return 1;
    }
    auto stub = api::TimeseriesCoreService::NewStub(channel);
    if (!syncSequences(stub.get())) {
        return 1;
    }

    std::cout << "\nETTh1 gRPC history demo\n"
              << "1. queryHistoryOverview\n"
              << "2. query first hour\n"
              << "3. query latest points\n"
              << "4. query custom [start, end)\n"
              << "q. quit\n";
    for (;;) {
        std::cout << "\nchoose an option: " << std::flush;
        std::string choice;
        if (!std::getline(std::cin, choice) || choice == "q" || choice == "Q") {
            break;
        }
        if (choice == "1") {
            queryOverview(stub.get());
        } else if (choice == "2") {
            queryData(stub.get(), kFirstTime, kFirstTime + kHour);
        } else if (choice == "3") {
            queryData(stub.get(), kLastTime, kLastTime + 1);
        } else if (choice == "4") {
            std::string startText;
            std::string endText;
            std::cout << "start_time (ms): " << std::flush;
            if (!std::getline(std::cin, startText)) {
                break;
            }
            std::cout << "end_time (ms): " << std::flush;
            if (!std::getline(std::cin, endText)) {
                break;
            }
            try {
                queryData(
                    stub.get(), std::stoll(startText), std::stoll(endText));
            } catch (const std::exception&) {
                std::cout << "timestamps must be integer milliseconds\n";
            }
        } else {
            std::cout << "unknown option; choose 1, 2, 3, 4 or q\n";
        }
    }
    std::cout << "demo finished\n";
    return 0;
}
