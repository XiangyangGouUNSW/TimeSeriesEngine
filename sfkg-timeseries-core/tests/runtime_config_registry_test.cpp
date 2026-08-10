#include <cassert>
#include <iostream>

#include "sfkg/timeseries/core/ingest_service.hpp"
#include "sfkg/timeseries/core/runtime_config_registry.hpp"
#include "sfkg/timeseries/core/window_service.hpp"

using namespace sfkg::timeseries::core;

int main() {
    RuntimeConfigRegistry registry;

    RuntimeInstanceConfig temperature;
    temperature.sequence_id = "temperature-1";
    temperature.data_source_id = "source-a";
    temperature.external_sequence_id = "temp";
    temperature.category_id = "temperature";
    temperature.data_type = "continuous";
    temperature.series_kind = SeriesKind::Continuous;

    auto result = registry.replaceInstanceConfigs({{temperature}});
    assert(result.code == OperationCode::Ok);
    assert(result.success_count == 1);
    assert(registry.findInstance("temperature-1").has_value());
    assert(registry.findInstance("temperature-1")->series_kind ==
           SeriesKind::Continuous);
    assert(registry.resolveSequenceId("source-a", "temp") ==
           std::optional<SequenceId>{"temperature-1"});

    IngestService ingest(registry);
    TimeseriesIngestData external_point;
    external_point.data_source_id = "source-a";
    external_point.external_sequence_id = "temp";
    external_point.time = 1'000;
    external_point.value = 25.0;
    const auto resolved = ingest.ingestAndResolveData({external_point});
    assert(resolved.operation.code == OperationCode::Ok);
    assert(resolved.resolved_data.points.size() == 1);
    assert(resolved.resolved_data.points.front().sequence_id ==
           "temperature-1");

    TimeseriesIngestData unknown_point;
    unknown_point.sequence_id = "unknown-sequence";
    unknown_point.time = 1'001;
    unknown_point.value = 26.0;
    const auto partially_resolved = ingest.ingestAndResolveData(
        {external_point, unknown_point});
    assert(partially_resolved.operation.code == OperationCode::PartialSuccess);
    assert(partially_resolved.resolved_data.points.size() == 1);
    assert(partially_resolved.operation.failed_count == 1);

    RuntimeInstanceConfig pressure;
    pressure.sequence_id = "pressure-1";
    pressure.data_source_id = "source-a";
    pressure.external_sequence_id = "pressure";
    pressure.category_id = "pressure";
    pressure.data_type = "continuous";
    pressure.series_kind = SeriesKind::Discrete;
    result = registry.upsertInstanceConfigs({{pressure}});
    assert(result.code == OperationCode::Ok);
    assert(registry.findInstance("temperature-1").has_value());
    assert(registry.findInstance("pressure-1").has_value());
    assert(registry.findInstance("pressure-1")->series_kind ==
           SeriesKind::Discrete);

    temperature.external_sequence_id = "temp-v2";
    result = registry.upsertInstanceConfigs({{temperature}});
    assert(result.code == OperationCode::Ok);
    assert(!registry.resolveSequenceId("source-a", "temp").has_value());
    assert(registry.resolveSequenceId("source-a", "temp-v2") ==
           std::optional<SequenceId>{"temperature-1"});
    assert(registry.findInstance("pressure-1").has_value());

    WindowService window;
    assert(window.windowSize() == WindowService::kDefaultWindowSizeMs);
    assert(window.configureWindowSize(1'000).code == OperationCode::Ok);
    assert(window.windowSize() == 1'000);
    const TimeseriesBatch window_batch{
        {{1'000, "temperature-1", 25.0},
         {1'500, "temperature-1", 26.0},
         {2'200, "temperature-1", 27.0}}};
    assert(window.buildTimeWindow(window_batch).code ==
           OperationCode::Ok);
    WindowQuery window_query;
    window_query.sequence_ids = {"temperature-1"};
    const auto window_result = window.queryWindowData(window_query);
    assert(window_result.operation.code == OperationCode::Ok);
    assert(window_result.data.sequence_values.at("temperature-1").size() == 2);

    ConstraintRule rule;
    rule.constraint_id = "temperature-range";
    rule.variable_mapping.emplace("x", "temperature-1");
    rule.lower_bound = -40.0;
    rule.upper_bound = 120.0;
    rule.terms.push_back({"x", 1.0, 0});
    result = registry.replaceConstraints({{{rule, true}}});
    assert(result.code == OperationCode::Ok);

    rule.upper_bound = 100.0;
    result = registry.upsertConstraints({{{rule, true}}});
    assert(result.code == OperationCode::Ok);

    assert(registry.enabledConstraints({"temperature-range"}).size() == 1);
    assert(registry.enabledConstraints({"unknown-constraint"}).empty());
    auto constraint_lookup = registry.lookupConstraints({
        "temperature-range", "unknown-constraint"});
    assert(constraint_lookup.enabled_rules.size() == 1);
    assert(constraint_lookup.missing_ids ==
           std::vector<std::string>{"unknown-constraint"});
    assert(constraint_lookup.disabled_ids.empty());

    result = registry.upsertConstraints({{{rule, false}}});
    assert(result.code == OperationCode::Ok);
    constraint_lookup = registry.lookupConstraints({"temperature-range"});
    assert(constraint_lookup.enabled_rules.empty());
    assert(constraint_lookup.missing_ids.empty());
    assert(constraint_lookup.disabled_ids ==
           std::vector<std::string>{"temperature-range"});

    result = registry.upsertConstraints({{{rule, true}}});
    assert(result.code == OperationCode::Ok);
    assert(registry.lookupConstraints({"temperature-range"})
               .enabled_rules.size() == 1);

    RuntimeRelationConfig relation;
    relation.relation_id = "temperature-pressure";
    relation.target_sequence_id = "pressure-1";
    relation.relation_type = "correlation";
    relation.confidence = 0.8;
    RuntimeRelationSource fixed_source;
    fixed_source.source_sequence_id = "temperature-1";
    fixed_source.weight = 1.0;
    fixed_source.lag = std::int64_t{2};
    relation.sources.push_back(fixed_source);
    result = registry.replaceRelations({{relation}});
    assert(result.code == OperationCode::Ok);

    relation.confidence = 0.9;
    result = registry.upsertRelations({{relation}});
    assert(result.code == OperationCode::Ok);

    RuntimeDerivedSeriesConfig derived;
    derived.derived_sequence_id = "temperature-pressure-sum";
    derived.enabled = true;
    derived.formula = DerivedLinearCombination{
        {{"temperature-1", 1.0}, {"pressure-1", 1.0}}, 0.0};
    result = registry.upsertDerivedSeriesConfigs({{derived}});
    assert(result.code == OperationCode::InvalidArgument);

    derived.formula = DerivedLinearCombination{{{"temperature-1", 1.0}}, 0.0};
    result = registry.upsertDerivedSeriesConfigs({{derived}});
    assert(result.code == OperationCode::Ok);
    assert(registry.allDerivedSeries().size() == 1);

    result = registry.replaceInstanceConfigs({{temperature, temperature}});
    assert(result.code == OperationCode::InvalidArgument);
    assert(registry.findInstance("temperature-1").has_value());

    std::cout << "runtime_config_registry_test passed\n";
    return 0;
}
