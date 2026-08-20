#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <unistd.h>
#include <variant>
#include <vector>

#include "sfkg/timeseries/core/history_query_service.hpp"
#include "sfkg/timeseries/core/internal/taos_client.hpp"

namespace core = sfkg::timeseries::core;

namespace {

bool expect(bool condition, const std::string& name) {
    std::cout << (condition ? "[PASS] " : "[FAIL] ") << name << '\n';
    return condition;
}

bool sameValue(const core::TimeseriesValue& actual, const core::TimeseriesValue& expected) {
    return actual == expected;
}

}  // namespace

int main() {
    setenv("SFKG_TAOS_HOST", "127.0.0.1", 1);
    setenv("SFKG_TAOS_PORT", "6030", 1);
    setenv("SFKG_TAOS_USER", "root", 1);
    setenv("SFKG_TAOS_PASSWORD", "taosdata", 1);
    const auto database = std::string("sfkg_taos_smoke_") +
        std::to_string(static_cast<long long>(getpid()));
    setenv("SFKG_TAOS_DB", database.c_str(), 1);

    core::internal::TaosClient client;
    bool passed = true;
    passed &= expect(
        client.ensureSchema().code == core::OperationCode::Ok,
        "ensure schema");

    constexpr core::Timestamp base = 1'700'000'000'000;
    core::TimeseriesBatch batch{
        {{base, "smoke-v2-double", 12.5},
         {base + 1, "smoke-v2-int", std::int64_t{9223372036854770000LL}},
         {base + 2, "smoke-v2-bool", true},
         {base + 3, "smoke-v2-string", std::string("稳定")},
         {base + 1, "smoke-v2-other", std::string("second")},
         {base + 2, "smoke-v2-other", std::string("third")}}};
    const auto write = client.insertRaw(batch);
    passed &= expect(
        write.code == core::OperationCode::Ok && write.success_count == batch.points.size(),
        "multi-sequence batch write");

    core::TimeseriesBatch read;
    const auto all = client.queryRaw(
        {"smoke-v2-double", "smoke-v2-int", "smoke-v2-bool", "smoke-v2-string", "smoke-v2-other"}, base, base + 4, &read);
    passed &= expect(
        all.code == core::OperationCode::Ok && read.points.size() == 6,
        "read four TimeseriesValue types");
    std::map<std::pair<std::string, core::Timestamp>, core::TimeseriesValue> values;
    for (const auto& point : read.points) {
        values[{point.sequence_id, point.time}] = point.value;
    }
    passed &= expect(
        sameValue(values[{"smoke-v2-double", base}], core::TimeseriesValue{12.5}) &&
            sameValue(values[{"smoke-v2-int", base + 1}], core::TimeseriesValue{std::int64_t{9223372036854770000LL}}) &&
            sameValue(values[{"smoke-v2-bool", base + 2}], core::TimeseriesValue{true}) &&
            sameValue(values[{"smoke-v2-string", base + 3}], core::TimeseriesValue{std::string("稳定")}) &&
            sameValue(values[{"smoke-v2-other", base + 1}], core::TimeseriesValue{std::string("second")}) &&
            sameValue(values[{"smoke-v2-other", base + 2}], core::TimeseriesValue{std::string("third")}),
        "DOUBLE/BIGINT/BOOL/STRING round trip");

    read.points.clear();
    const auto boundary = client.queryRaw(
        {"smoke-v2-double", "smoke-v2-int", "smoke-v2-bool", "smoke-v2-string", "smoke-v2-other"}, base, base + 3, &read);
    passed &= expect(
        boundary.code == core::OperationCode::Ok && read.points.size() == 5,
        "[start, end) boundary");

    const std::vector<std::string> requested_order{
        "smoke-v2-other", "smoke-v2-int", "smoke-v2-double",
        "smoke-v2-bool", "smoke-v2-string"};
    read.points.clear();
    const auto ordered = client.queryRaw(
        requested_order, base, base + 4, &read);
    std::vector<std::string> at_base_plus_one;
    std::vector<std::string> at_base_plus_two;
    for (const auto& point : read.points) {
        if (point.time == base + 1) {
            at_base_plus_one.push_back(point.sequence_id);
        } else if (point.time == base + 2) {
            at_base_plus_two.push_back(point.sequence_id);
        }
    }
    passed &= expect(
        ordered.code == core::OperationCode::Ok &&
            at_base_plus_one == std::vector<std::string>{
                "smoke-v2-other", "smoke-v2-int"} &&
            at_base_plus_two == std::vector<std::string>{
                "smoke-v2-other", "smoke-v2-bool"},
        "queryRaw preserves requested sequence order");

    read.points.clear();
    const auto bucketed = client.queryRaw(
        {"smoke-v2-other"}, base, base + 4, &read, 2);
    passed &= expect(
        bucketed.code == core::OperationCode::Ok &&
            read.points.size() == 2 &&
            read.points[0].time == base &&
            read.points[1].time == base + 2 &&
            sameValue(read.points[0].value, core::TimeseriesValue{
                std::string("second")}) &&
            sameValue(read.points[1].value, core::TimeseriesValue{
                std::string("third")}),
        "queryRaw applies millisecond granularity with last value");

    read.points.clear();
    const auto empty = client.queryRaw(
        {"smoke-double"}, base + 100, base + 200, &read);
    passed &= expect(
        empty.code == core::OperationCode::Ok && read.points.empty(),
        "empty result is successful");

    core::RuntimeConfigRegistry registry;
    core::RuntimeConfigSnapshot<core::RuntimeInstanceConfig> snapshot;
    snapshot.items.push_back({"smoke-v2-double", "test", "double", "", ""});
    snapshot.items.push_back({"smoke-v2-int", "test", "int", "", ""});
    snapshot.items.push_back({"smoke-v2-bool", "test", "bool", "", ""});
    snapshot.items.push_back({"smoke-v2-string", "test", "string", "", ""});
    snapshot.items.push_back({"smoke-v2-other", "test", "other", "", ""});
    passed &= expect(
        registry.replaceInstanceConfigs(snapshot).code == core::OperationCode::Ok,
        "register test sequences");
    core::HistoryQueryService history(registry, client);
    const auto overview = history.queryHistoryOverview(
        {requested_order, base, base + 4});
    std::vector<std::string> overview_order;
    for (const auto& series : overview.overview.series) {
        overview_order.push_back(series.sequence_id);
    }
    passed &= expect(
        overview.operation.code == core::OperationCode::Ok &&
            overview_order == requested_order,
        "queryHistoryOverview preserves requested sequence order");
    const auto typed_history = history.queryHistoryData(
        {{"smoke-v2-double"}, base, base + 4, std::nullopt});
    passed &= expect(
        typed_history.operation.code == core::OperationCode::Ok &&
            typed_history.data.points.size() == 1 &&
            sameValue(typed_history.data.points.front().value,
                      core::TimeseriesValue{12.5}),
        "history query uses registered physical value type");
    const auto mixed_history = history.queryHistoryData(
        {{"smoke-v2-double", "smoke-v2-int"}, base, base + 4, std::nullopt});
    passed &= expect(
        mixed_history.operation.code == core::OperationCode::Ok &&
            mixed_history.data.points.size() == 2,
        "history query preserves mixed-type fallback");
    const auto not_found = history.queryHistoryData(
        {{"not-registered"}, base, base + 1, std::nullopt});
    passed &= expect(
        not_found.operation.code == core::OperationCode::NotFound,
        "unregistered sequence is NotFound");

    setenv("SFKG_TAOS_HOST", "invalid.invalid", 1);
    setenv("SFKG_TAOS_PORT", "6030", 1);
    core::internal::TaosClient unavailable_client;
    passed &= expect(
        unavailable_client.ensureSchema().code == core::OperationCode::Unavailable,
        "TDengine unavailable is Unavailable");

    passed &= expect(
        client.dropDatabaseForTesting().code == core::OperationCode::Ok,
        "cleanup isolated smoke database");

    std::cout << (passed ? "taos client smoke passed\n" : "taos client smoke failed\n");
    return passed ? 0 : 1;
}
