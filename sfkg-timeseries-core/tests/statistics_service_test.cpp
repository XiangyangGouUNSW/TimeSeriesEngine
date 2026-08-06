#include <cassert>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <string>

#include "sfkg/timeseries/core/statistics_service.hpp"

using namespace sfkg::timeseries::core;

namespace {

double metricAsDouble(
    const StatisticsResult& result,
    const SequenceId& sequence_id,
    const std::string& name) {
    const auto sequence = result.sequence_metrics.find(sequence_id);
    assert(sequence != result.sequence_metrics.end());
    const auto metric = sequence->second.find(name);
    assert(metric != sequence->second.end());
    if (const auto* value = std::get_if<double>(&metric->second)) {
        return *value;
    }
    if (const auto* value = std::get_if<std::int64_t>(&metric->second)) {
        return static_cast<double>(*value);
    }
    assert(false && "metric is not numeric");
    return 0.0;
}

RuntimeRelationConfig relationFor(
    SequenceId target,
    std::initializer_list<SequenceId> sources) {
    RuntimeRelationConfig relation;
    relation.relation_id = "target-relation";
    relation.target_sequence_id = std::move(target);
    relation.relation_type = "correlation";
    relation.enabled = true;
    for (auto source : sources) {
        relation.sources.push_back({std::move(source), 1.0, std::monostate{}});
    }
    return relation;
}

}  // namespace

int main() {
    StatisticsService service;

    WindowData window;
    window.window_start_time = 1'000;
    window.window_end_time = 4'000;
    window.sequence_values["temperature-1"] = {
        {1'000, "temperature-1", 1.0},
        {2'000, "temperature-1", 2.0},
        {3'000, "temperature-1", 3.0},
        {4'000, "temperature-1", 4.0}};

    auto result = service.computeBasicStatistics(window);
    assert(result.operation.code == OperationCode::Ok);
    assert(result.operation.success_count == 1);
    assert(metricAsDouble(result, "temperature-1", "count") == 4.0);
    assert(metricAsDouble(result, "temperature-1", "sum") == 10.0);
    assert(metricAsDouble(result, "temperature-1", "mean") == 2.5);
    assert(metricAsDouble(result, "temperature-1", "min") == 1.0);
    assert(metricAsDouble(result, "temperature-1", "max") == 4.0);
    assert(metricAsDouble(result, "temperature-1", "variance") == 1.25);
    assert(std::abs(
               metricAsDouble(result, "temperature-1", "stddev") -
               std::sqrt(1.25)) < 1e-12);
    assert(metricAsDouble(result, "temperature-1", "first_time") == 1'000.0);
    assert(metricAsDouble(result, "temperature-1", "last_time") == 4'000.0);

    window.sequence_values["label-1"] = {
        {1'000, "label-1", std::string("normal")},
        {2'000, "label-1", std::string("warning")}};
    result = service.computeBasicStatistics(window);
    assert(result.operation.code == OperationCode::PartialSuccess);
    assert(result.operation.success_count == 1);
    assert(result.operation.failed_count == 1);
    assert(metricAsDouble(result, "label-1", "count") == 2.0);
    assert(result.sequence_metrics.at("label-1").count("mean") == 0);

    AlignedWindowData aligned;
    aligned.window_start_time = 1;
    aligned.window_end_time = 4;
    aligned.samples = {
        {1, {{"target-1", 1.0}, {"source-positive", 2.0},
             {"source-negative", 8.0}}},
        {2, {{"target-1", 2.0}, {"source-positive", 4.0},
             {"source-negative", 6.0}}},
        {3, {{"target-1", 3.0}, {"source-positive", 6.0},
             {"source-negative", 4.0}}},
        {4, {{"target-1", 4.0}, {"source-positive", 8.0},
             {"source-negative", 2.0}}}};

    result = service.computeBasicStatistics(
        aligned,
        relationFor("target-1", {"source-positive", "source-negative"}));
    assert(result.operation.code == OperationCode::Ok);
    assert(result.operation.success_count == 2);
    assert(result.correlation_vector.has_value());
    assert(metricAsDouble(result, "target-1", "count") == 4.0);
    assert(metricAsDouble(result, "target-1", "mean") == 2.5);
    assert(result.correlation_vector->dependent_sequence_id == "target-1");
    assert(result.correlation_vector->correlations.size() == 2);
    assert(std::abs(
               result.correlation_vector->correlations[0].coefficient - 1.0) <
           1e-12);
    assert(std::abs(
               result.correlation_vector->correlations[1].coefficient + 1.0) <
           1e-12);

    auto invalid_relation = relationFor("target-1", {"source-positive"});
    invalid_relation.enabled = false;
    result = service.computeBasicStatistics(aligned, invalid_relation);
    assert(result.operation.code == OperationCode::FailedPrecondition);

    auto constant_relation = relationFor("target-1", {"source-positive"});
    for (auto& sample : aligned.samples) {
        sample.values["source-positive"] = 2.0;
    }
    result = service.computeBasicStatistics(aligned, constant_relation);
    assert(result.operation.code == OperationCode::InvalidArgument);
    assert(result.correlation_vector.has_value());
    assert(result.correlation_vector->correlations.empty());

    std::cout << "statistics_service_test passed\n";
    return 0;
}
