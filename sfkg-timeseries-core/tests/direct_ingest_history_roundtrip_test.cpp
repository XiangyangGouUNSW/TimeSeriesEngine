#include <cassert>
#include <cstdlib>
#include <iostream>
#include <string>
#include <unistd.h>

#include "sfkg/timeseries/core/history_query_service.hpp"
#include "sfkg/timeseries/core/ingest_service.hpp"
#include "sfkg/timeseries/core/internal/taos_client.hpp"
#include "sfkg/timeseries/core/runtime_config_registry.hpp"
#include "sfkg/timeseries/core/storage_service.hpp"
#include "sfkg/timeseries/core/window_service.hpp"

namespace core = sfkg::timeseries::core;

class DatabaseCleanup {
public:
    explicit DatabaseCleanup(core::internal::TaosClient& client)
        : client_(client) {}

    ~DatabaseCleanup() {
        const auto result = client_.dropDatabaseForTesting();
        if (result.code != core::OperationCode::Ok) {
            std::cerr << "test database cleanup failed: "
                      << result.message << '\n';
        }
    }

private:
    core::internal::TaosClient& client_;
};

int main() {
    // Use a database unique to this process so this local round-trip test does
    // not depend on data left by another test or demo.
    setenv(
        "SFKG_TAOS_DB",
        (std::string("sfkg_direct_ingest_test_") +
         std::to_string(static_cast<long long>(getpid()))).c_str(),
        1);
    setenv("SFKG_TAOS_HOST", "127.0.0.1", 1);
    setenv("SFKG_TAOS_PORT", "6030", 1);
    setenv("SFKG_TAOS_USER", "root", 1);
    setenv("SFKG_TAOS_PASSWORD", "taosdata", 1);
    setenv("SFKG_TAOS_KEEP_DAYS", "20000", 1);

    core::RuntimeConfigRegistry registry;
    const auto config = registry.upsertInstanceConfigs({{
        {"direct-sequence-a", "direct-source", "external-a", "temperature", "double"},
        {"direct-sequence-b", "direct-source", "external-b", "pressure", "double"},
    }});
    assert(config.code == core::OperationCode::Ok);

    core::internal::TaosClient taos;
    DatabaseCleanup cleanup(taos);
    assert(taos.ensureSchema().code == core::OperationCode::Ok);

    core::IngestService ingest(registry);
    core::StorageService storage(taos);
    core::WindowService window;
    core::HistoryQueryService history(registry, taos);

    constexpr core::Timestamp base = 1'800'000'000'000;

    // TDengine child tables are database-global. Reusing the same sequence ID
    // in two projects must therefore produce two independent child tables.
    const core::TimeseriesBatch project_a_batch{
        {{base, "project-shared-sequence", 1.0, "project-a"}},
        "project-a"};
    const core::TimeseriesBatch project_b_batch{
        {{base, "project-shared-sequence", 2.0, "project-b"}},
        "project-b"};
    assert(taos.insertRaw("project-a", project_a_batch).code ==
           core::OperationCode::Ok);
    assert(taos.insertRaw("project-b", project_b_batch).code ==
           core::OperationCode::Ok);
    core::TimeseriesBatch project_a_read;
    core::TimeseriesBatch project_b_read;
    assert(taos.queryRaw(
               "project-a", {"project-shared-sequence"}, base, base + 1,
               &project_a_read).code == core::OperationCode::Ok);
    assert(taos.queryRaw(
               "project-b", {"project-shared-sequence"}, base, base + 1,
               &project_b_read).code == core::OperationCode::Ok);
    assert(project_a_read.points.size() == 1);
    assert(project_b_read.points.size() == 1);
    assert(std::get<double>(project_a_read.points.front().value) == 1.0);
    assert(std::get<double>(project_b_read.points.front().value) == 2.0);

    const core::TimeseriesIngestData input_a{
        std::nullopt, "direct-source", "external-a", base, 21.5};
    const core::TimeseriesIngestData input_b{
        std::nullopt, "direct-source", "external-b", base + 500, 101.25};
    const auto resolved = ingest.ingestAndResolveData({input_a, input_b});
    assert(resolved.operation.code == core::OperationCode::Ok);
    assert(resolved.resolved_data.points.size() == 2);

    // Mirror the non-gRPC ingest coordinator: persist cold data and update the
    // hot window using the same resolved batch.
    const auto stored = storage.writeRawData(resolved.resolved_data);
    assert(stored.code == core::OperationCode::Ok);
    const auto windowed = window.buildTimeWindow(resolved.resolved_data, 1'000);
    assert(windowed.code == core::OperationCode::Ok);

    const auto raw = history.queryHistoryData({
        {"direct-sequence-a", "direct-sequence-b"}, base, base + 1'000,
        std::nullopt});
    assert(raw.operation.code == core::OperationCode::Ok);
    assert(raw.data.points.size() == 2);

    const auto overview = history.queryHistoryOverview({
        {"direct-sequence-a", "direct-sequence-b"}, base, base + 1'000});
    assert(overview.operation.code == core::OperationCode::Ok);
    assert(overview.overview.total_point_count == 2);
    assert(overview.overview.series.size() == 2);

    core::WindowQuery window_query;
    window_query.sequence_ids = {"direct-sequence-a", "direct-sequence-b"};
    const auto hot = window.queryWindowData(window_query);
    assert(hot.operation.code == core::OperationCode::Ok);
    assert(hot.operation.success_count == 2);

    std::cout << "direct ingest/history round-trip passed\n";
    return 0;
}
