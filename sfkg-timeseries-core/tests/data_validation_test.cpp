#include <cassert>
#include <cmath>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "sfkg/timeseries/core/data_validation.hpp"
#include "sfkg/timeseries/core/ingest_service.hpp"
#include "sfkg/timeseries/core/statistics_service.hpp"
#include "sfkg/timeseries/core/window_service.hpp"

using namespace sfkg::timeseries::core;

namespace {

RuntimeInstanceConfig instance(
    std::string sequence_id,
    std::string external_id,
    std::string data_type) {
    return {std::move(sequence_id), "source", std::move(external_id), "",
            std::move(data_type), SeriesKind::Continuous, "project-a"};
}

TimeseriesIngestData point(
    std::string sequence_id,
    Timestamp time,
    TimeseriesValue value,
    ProjectId project_id = "project-a") {
    return {std::move(sequence_id), "", "", time, std::move(value),
            std::move(project_id)};
}

}  // namespace

int main() {
    RuntimeConfigRegistry registry;
    assert(registry.replaceInstanceConfigs({
        {instance("temperature", "temp", "double"),
         instance("state", "state", "int64")},
        "project-a"}).code == OperationCode::Ok);

    // A business constraint must never turn a structurally usable point into
    // invalid input. The value 999 is deliberately outside this rule.
    ConstraintRule rule;
    rule.constraint_id = "temperature-limit";
    rule.variable_mapping.emplace("x", "temperature");
    rule.lower_bound = -10.0;
    rule.upper_bound = 10.0;
    rule.terms.push_back({"x", 1.0, 0});
    RuntimeConstraintConfig constraint{rule, true, "project-a"};
    constraint.rule.project_id = "project-a";
    assert(registry.replaceConstraints({{constraint}, "project-a"}).code ==
           OperationCode::Ok);

    IngestService ingest(registry);
    std::vector<TimeseriesIngestData> repeated;
    repeated.reserve(2'000);
    for (Timestamp time = 0; time < 1'000; ++time) {
        repeated.push_back(point("temperature", time, 999.0));
        repeated.push_back(point("state", time, std::int64_t{1}));
    }
    const auto valid = ingest.ingestAndResolveData("project-a", repeated);
    assert(valid.operation.code == OperationCode::Ok);
    assert(valid.operation.success_count == repeated.size());

    auto nan = point(
        "temperature", 1'001, std::numeric_limits<double>::quiet_NaN());
    auto wrong_type = point("temperature", 1'002, std::int64_t{3});
    auto wrong_project = point("temperature", 1'003, 3.0, "project-b");
    const auto partial = ingest.ingestAndResolveData(
        "project-a", {point("temperature", 1'000, 3.0), nan,
                      wrong_type, wrong_project});
    assert(partial.operation.code == OperationCode::PartialSuccess);
    assert(partial.operation.success_count == 1);
    assert(partial.operation.failed_count == 3);
    assert(partial.resolved_data.points.size() == 1);

    TimeseriesIngestData external{
        std::nullopt, "source", "temp", 2'000, 5.0, "project-a"};
    const auto external_result = ingest.ingestAndResolveData(
        "project-a", {external, external});
    assert(external_result.operation.code == OperationCode::Ok);
    assert(external_result.resolved_data.points.front().sequence_id ==
           "temperature");

    auto mismatched_identifiers = point("temperature", 2'001, 5.0);
    mismatched_identifiers.data_source_id = "source";
    mismatched_identifiers.external_sequence_id = "state";
    const auto mismatch = ingest.ingestAndResolveData(
        "project-a", {mismatched_identifiers});
    assert(mismatch.operation.code == OperationCode::InvalidArgument);
    assert(mismatch.operation.success_count == 0);

    const RawTimeseriesPoint finite{1, "temperature", 1.0, "project-a"};
    const RawTimeseriesPoint infinite{
        1, "temperature", std::numeric_limits<double>::infinity(),
        "project-a"};
    assert(validation::validateRawPoint(finite) == nullptr);
    assert(validation::validateRawPoint(infinite) != nullptr);
    const RawTimeseriesPoint wrong_project_raw{
        2, "temperature", 2.0, "project-b"};
    assert(validation::validateRawPoint(
               "project-a", wrong_project_raw) != nullptr);

    WindowData malformed_window;
    malformed_window.window_start_time = 0;
    malformed_window.window_end_time = 10;
    malformed_window.sequence_values["temperature"] = {
        {1, "other-sequence", 1.0, ""}};
    assert(validation::validateWindowData("project-a", malformed_window) !=
           nullptr);
    StatisticsService statistics;
    assert(statistics.computeBasicStatistics(
               "project-a", malformed_window).operation.code ==
           OperationCode::InvalidArgument);

    AlignedWindowData malformed_aligned;
    malformed_aligned.window_start_time = 0;
    malformed_aligned.window_end_time = 10;
    malformed_aligned.samples = {
        {2, {{"temperature", 2.0}}},
        {1, {{"temperature", 1.0}}}};
    assert(validation::validateAlignedWindowData(
               "project-a", malformed_aligned) != nullptr);
    assert(statistics.computeBasicStatistics(
               "project-a", malformed_aligned,
               RuntimeRelationConfig{})
               .operation.code == OperationCode::InvalidArgument);

    // Window validation happens in the same pass that groups new points. An
    // invalid batch is rejected before any hot-window state is changed.
    WindowService window;
    assert(window.configureWindowSize("project-a", 10'000).code ==
           OperationCode::Ok);
    assert(window.buildTimeWindow(
               "project-a", TimeseriesBatch{{finite}, "project-a"}).code ==
           OperationCode::Ok);
    assert(window.buildTimeWindow(
               "project-a", TimeseriesBatch{{infinite}, "project-a"}).code ==
           OperationCode::InvalidArgument);
    assert(window.buildTimeWindow(
               "project-a",
               TimeseriesBatch{{wrong_project_raw}, "project-b"}).code ==
           OperationCode::InvalidArgument);
    WindowQuery query;
    query.project_id = "project-a";
    query.sequence_ids = {"temperature"};
    const auto after_invalid = window.queryWindowData("project-a", query);
    assert(after_invalid.operation.code == OperationCode::Ok);
    assert(after_invalid.data.sequence_values.at("temperature").size() == 1);

    query.sequence_ids = {""};
    assert(window.queryWindowData("project-a", query).operation.code ==
           OperationCode::InvalidArgument);

    std::cout << "data_validation_test passed\n";
    return 0;
}
