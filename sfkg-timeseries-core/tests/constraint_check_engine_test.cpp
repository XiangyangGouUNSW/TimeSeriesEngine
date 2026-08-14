#include <cassert>
#include <cstdint>
#include <iostream>

#include "sfkg/timeseries/core/constraint_check_engine.hpp"

using namespace sfkg::timeseries::core;

ConstraintRule singleSequenceRule() {
    ConstraintRule rule;
    rule.constraint_id = "temperature-range";
    rule.variable_mapping.emplace("x", "temperature-1");
    rule.lower_bound = 0.0;
    rule.upper_bound = 10.0;
    rule.terms.push_back({"x", 1.0, 0});
    return rule;
}

int main() {
    ConstraintCheckEngine engine;

    const auto single_rule = singleSequenceRule();
    WindowData window;
    window.window_start_time = 0;
    window.window_end_time = 3;
    window.sequence_values["temperature-1"] = {
        {0, "temperature-1", 8.0},
        {1, "temperature-1", 11.0},
        {2, "temperature-1", 10.0}};

    auto result = engine.checkConstraints({single_rule}, window);
    assert(result.operation.code == OperationCode::Ok);
    assert(result.operation.success_count == 3);
    assert(!result.satisfied);
    assert(result.violations.size() == 1);
    assert(result.violations.front().anchor_time == 1);
    assert(result.violations.front().evaluated_value == 11.0);

    result = engine.checkConstraints(
        {single_rule}, window, ConstraintCheckRange{2, 2});
    assert(result.operation.code == OperationCode::Ok);
    assert(result.operation.success_count == 1);
    assert(result.satisfied);

    ConstraintRule multi_sequence = single_rule;
    multi_sequence.constraint_id = "temperature-pressure";
    multi_sequence.variable_mapping.emplace("p", "pressure-1");
    multi_sequence.terms.push_back({"p", -0.5, 0});
    result = engine.checkConstraints({multi_sequence}, window);
    assert(result.operation.code == OperationCode::FailedPrecondition);

    ConstraintRule offset_rule = single_rule;
    offset_rule.constraint_id = "temperature-change";
    offset_rule.lower_bound = -2.0;
    offset_rule.upper_bound = 2.0;
    offset_rule.terms = {{"x", 1.0, 0}, {"x", -1.0, 1}};
    result = engine.checkConstraints({offset_rule}, window);
    assert(result.operation.code == OperationCode::Ok);
    assert(result.operation.success_count == 2);
    assert(!result.satisfied);
    assert(result.violations.size() == 1);
    assert(result.violations.front().anchor_time == 0);
    assert(result.violations.front().evaluated_value == -3.0);

    ConstraintRule aligned_rule;
    aligned_rule.constraint_id = "temperature-pressure-aligned";
    aligned_rule.variable_mapping = {
        {"temperature", "temperature-1"},
        {"pressure", "pressure-1"}};
    aligned_rule.lower_bound = -1.0;
    aligned_rule.upper_bound = 1.0;
    aligned_rule.terms = {
        {"temperature", 1.0, 0},
        {"pressure", -0.5, 0}};

    AlignedWindowData aligned;
    aligned.window_start_time = 0;
    aligned.window_end_time = 3;
    aligned.samples = {
        {0, {{"temperature-1", 10.0}, {"pressure-1", 20.0}}},
        {1, {{"temperature-1", 11.0}, {"pressure-1", 22.0}}},
        {2, {{"temperature-1", 13.0}, {"pressure-1", 24.0}}}};

    result = engine.checkConstraints({aligned_rule}, aligned);
    assert(result.operation.code == OperationCode::Ok);
    assert(result.operation.success_count == 3);
    assert(result.satisfied);

    aligned.samples[1].values.erase("pressure-1");
    result = engine.checkConstraints({aligned_rule}, aligned);
    assert(result.operation.code == OperationCode::Ok);
    assert(result.operation.success_count == 2);
    assert(result.pending_count == 1);
    assert(result.satisfied);

    aligned.samples[1].values.emplace("pressure-1", 22.0);
    aligned_rule.constraint_id = "temperature-next-pressure";
    aligned_rule.terms = {
        {"temperature", 1.0, 0},
        {"pressure", -0.5, 1}};
    aligned_rule.lower_bound = -2.0;
    aligned_rule.upper_bound = 0.0;
    result = engine.checkConstraints({aligned_rule}, aligned);
    assert(result.operation.code == OperationCode::Ok);
    assert(result.operation.success_count == 2);
    assert(result.satisfied);

    std::cout << "constraint_check_engine_test passed\n";
    return 0;
}
