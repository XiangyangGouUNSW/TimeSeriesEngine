#include <cassert>
#include <iostream>

#include "sfkg/timeseries/core/runtime_config_registry.hpp"
#include "sfkg/timeseries/core/window_service.hpp"

using namespace sfkg::timeseries::core;

namespace {

RuntimeInstanceConfig instance(
    const ProjectId& project_id,
    const std::string& external_id) {
    RuntimeInstanceConfig config;
    config.project_id = project_id;
    config.sequence_id = "shared-sequence";
    config.data_source_id = "source";
    config.external_sequence_id = external_id;
    config.data_type = "continuous";
    config.series_kind = SeriesKind::Continuous;
    return config;
}

TimeseriesBatch batch(
    const ProjectId& project_id,
    Timestamp time,
    double value) {
    return TimeseriesBatch{
        {{time, "shared-sequence", value, project_id}}, project_id};
}

}  // namespace

int main() {
    RuntimeConfigRegistry registry;
    assert(registry.replaceInstanceConfigs({
        {instance("project-a", "a")}, "project-a"}).code ==
           OperationCode::Ok);
    assert(registry.replaceInstanceConfigs({
        {instance("project-b", "b")}, "project-b"}).code ==
           OperationCode::Ok);

    assert(registry.findInstance("project-a", "shared-sequence")
               ->external_sequence_id == "a");
    assert(registry.findInstance("project-b", "shared-sequence")
               ->external_sequence_id == "b");
    assert(!registry.resolveSequenceId("project-a", "source", "b"));
    assert(registry.resolveSequenceId("project-b", "source", "b") ==
           std::optional<SequenceId>{"shared-sequence"});

    ConstraintRule rule;
    rule.constraint_id = "shared-constraint";
    rule.variable_mapping.emplace("x", "shared-sequence");
    rule.lower_bound = -10.0;
    rule.upper_bound = 10.0;
    rule.terms.push_back({"x", 1.0, 0});
    for (const auto& project_id : {ProjectId{"project-a"}, ProjectId{"project-b"}}) {
        RuntimeConstraintConfig config;
        config.rule = rule;
        config.rule.project_id = project_id;
        config.enabled = true;
        config.project_id = project_id;
        assert(registry.upsertConstraints({{config}, project_id}).code ==
               OperationCode::Ok);
    }
    assert(registry.lookupConstraints("project-a", {"shared-constraint"})
               .enabled_rules.size() == 1);
    assert(registry.lookupConstraints("project-b", {"shared-constraint"})
               .enabled_rules.size() == 1);

    for (const auto& project_id : {ProjectId{"project-a"}, ProjectId{"project-b"}}) {
        RuntimeDerivedSeriesConfig derived;
        derived.derived_sequence_id = "shared-derived";
        derived.enabled = true;
        derived.project_id = project_id;
        derived.formula = DerivedLinearCombination{
            {{"shared-sequence", 1.0}}, 0.0};
        assert(registry.upsertDerivedSeriesConfigs({{derived}, project_id}).code ==
               OperationCode::Ok);
    }
    assert(registry.allDerivedSeries("project-a").size() == 1);
    assert(registry.allDerivedSeries("project-b").size() == 1);

    WindowService window;
    assert(window.configureWindowSize("project-a", 100).code ==
           OperationCode::Ok);
    assert(window.configureWindowSize("project-b", 1'000).code ==
           OperationCode::Ok);
    assert(window.buildTimeWindow("project-a", batch("project-a", 100, 1.0))
               .code == OperationCode::Ok);
    assert(window.buildTimeWindow("project-b", batch("project-b", 100, 2.0))
               .code == OperationCode::Ok);

    WindowQuery query;
    query.sequence_ids = {"shared-sequence"};
    query.project_id = "project-a";
    const auto a = window.queryWindowData("project-a", query);
    query.project_id = "project-b";
    const auto b = window.queryWindowData("project-b", query);
    assert(a.data.project_id == "project-a");
    assert(b.data.project_id == "project-b");
    assert(std::get<double>(a.data.sequence_values.at("shared-sequence").front().value) ==
           1.0);
    assert(std::get<double>(b.data.sequence_values.at("shared-sequence").front().value) ==
           2.0);

    std::cout << "project_isolation_test passed\n";
    return 0;
}
