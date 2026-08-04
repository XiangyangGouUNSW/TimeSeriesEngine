#include <cassert>
#include <iostream>

#include "sfkg/timeseries/core/runtime_config_registry.hpp"

using namespace sfkg::timeseries::core;

int main() {
    RuntimeConfigRegistry registry;

    RuntimeInstanceConfig temperature;
    temperature.sequence_id = "temperature-1";
    temperature.data_source_id = "source-a";
    temperature.external_sequence_id = "temp";
    temperature.category_id = "temperature";
    temperature.data_type = "continuous";

    auto result = registry.replaceInstanceConfigs({{temperature}});
    assert(result.code == OperationCode::Ok);
    assert(result.success_count == 1);
    assert(registry.findInstance("temperature-1").has_value());
    assert(registry.resolveSequenceId("source-a", "temp") ==
           std::optional<SequenceId>{"temperature-1"});

    ConstraintRule rule;
    rule.constraint_id = "temperature-range";
    rule.variable_mapping.emplace("x", "temperature-1");
    rule.lower_bound = -40.0;
    rule.upper_bound = 120.0;
    rule.terms.push_back({"x", 1.0, 0});
    result = registry.replaceConstraints({{{rule, true}}});
    assert(result.code == OperationCode::Ok);

    assert(registry.enabledConstraints({"temperature-range"}).size() == 1);
    assert(registry.enabledConstraints({"unknown-constraint"}).empty());

    RuntimeRelationConfig relation;
    relation.relation_id = "temperature-pressure";
    relation.target_category_id = "pressure";
    relation.relation_type = "correlation";
    relation.confidence = 0.8;
    RuntimeRelationSource fixed_source;
    fixed_source.source_category_id = "temperature";
    fixed_source.weight = 1.0;
    fixed_source.lag = std::int64_t{2};
    relation.sources.push_back(fixed_source);
    RuntimeRelationSource ranged_source;
    ranged_source.source_category_id = "humidity";
    ranged_source.weight = 0.5;
    ranged_source.lag = RelationLagRange{0, 10};
    relation.sources.push_back(ranged_source);
    RuntimeRelationSource unbounded_source;
    unbounded_source.source_category_id = "calendar";
    unbounded_source.weight = 0.2;
    relation.sources.push_back(unbounded_source);
    result = registry.replaceRelations({{relation}});
    assert(result.code == OperationCode::Ok);

    result = registry.replaceInstanceConfigs({{temperature, temperature}});
    assert(result.code == OperationCode::InvalidArgument);
    assert(registry.findInstance("temperature-1").has_value());

    std::cout << "runtime_config_registry_test passed\n";
    return 0;
}
