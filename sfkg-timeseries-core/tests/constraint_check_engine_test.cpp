#include <cassert>
#include <cstdint>
#include <cmath>
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

    // Bounds are independently optional; absence is represented explicitly,
    // not with a magic largest/smallest floating-point value.
    ConstraintRule upper_only = single_rule;
    upper_only.constraint_id = "upper-only";
    upper_only.lower_bound.reset();
    upper_only.upper_bound = 10.0;
    result = engine.checkConstraints({upper_only}, window);
    assert(result.operation.code == OperationCode::Ok);
    assert(result.violations.size() == 1);
    assert(!result.violations.front().lower_bound);

    ConstraintRule lower_only = single_rule;
    lower_only.constraint_id = "lower-only";
    lower_only.lower_bound = 9.0;
    lower_only.upper_bound.reset();
    result = engine.checkConstraints({lower_only}, window);
    assert(result.operation.code == OperationCode::Ok);
    assert(result.violations.size() == 1);
    assert(!result.violations.front().upper_bound);

    ConstraintRule unbounded = single_rule;
    unbounded.constraint_id = "unbounded";
    unbounded.lower_bound.reset();
    unbounded.upper_bound.reset();
    result = engine.checkConstraints({unbounded}, window);
    assert(result.operation.code == OperationCode::InvalidArgument);

    // avg/max/min terms may form a linear combination, evaluated once over
    // each concrete sequence's raw current window.
    ConstraintRule aggregate;
    aggregate.constraint_id = "aggregate-combination";
    aggregate.variable_mapping = {{"x", "temperature-1"}};
    aggregate.upper_bound = 28.0;
    aggregate.terms = {
        {"x", 1.0, 0, ConstraintAggregation::Average},
        {"x", 1.0, 0, ConstraintAggregation::Maximum},
        {"x", 1.0, 0, ConstraintAggregation::Minimum}};
    result = engine.checkConstraints({aggregate}, window);
    assert(result.operation.code == OperationCode::Ok);
    assert(result.evaluated_count == 1);
    assert(!result.satisfied);
    assert(result.violations.size() == 1);
    assert(std::abs(result.violations.front().evaluated_value -
                    (28.0 + 2.0 / 3.0)) < 1e-12);
    assert(result.violations.front().term_values[0].aggregation ==
           ConstraintAggregation::Average);

    ConstraintRule mixed_terms = aggregate;
    mixed_terms.constraint_id = "mixed-terms";
    mixed_terms.terms.push_back(
        {"x", 1.0, 0, ConstraintAggregation::Sample});
    result = engine.checkConstraints({mixed_terms}, window);
    assert(result.operation.code == OperationCode::InvalidArgument);

    // OR is applied to each Rule's whole-window result. These two rules fail
    // at different timestamps, therefore their OR clause still fails.
    WindowData or_window;
    or_window.window_start_time = 0;
    or_window.window_end_time = 2;
    or_window.sequence_values["temperature-1"] = {
        {0, "temperature-1", 11.0},
        {1, "temperature-1", 5.0}};
    ConstraintRule or_upper = upper_only;
    or_upper.constraint_id = "or-upper";
    or_upper.or_group_id = "range-choice";
    ConstraintRule or_lower = lower_only;
    or_lower.constraint_id = "or-lower";
    or_lower.lower_bound = 6.0;
    or_lower.or_group_id = "range-choice";
    result = engine.checkConstraints({or_upper, or_lower}, or_window);
    assert(result.operation.code == OperationCode::Ok);
    assert(!result.satisfied);
    assert(result.violations.size() == 2);

    // Different rule types may be alternatives in one OR clause.
    ConstraintRule aggregate_alternative;
    aggregate_alternative.constraint_id = "average-alternative";
    aggregate_alternative.variable_mapping = {{"x", "temperature-1"}};
    aggregate_alternative.lower_bound = 7.0;
    aggregate_alternative.terms = {
        {"x", 1.0, 0, ConstraintAggregation::Average}};
    aggregate_alternative.or_group_id = "mixed-choice";
    or_upper.or_group_id = "mixed-choice";
    result = engine.checkConstraints(
        {or_upper, aggregate_alternative}, or_window);
    assert(result.operation.code == OperationCode::Ok);
    assert(result.satisfied);
    assert(result.violations.empty());

    // Outer clauses remain AND and only failed clauses contribute reports.
    ConstraintRule independent_failure = upper_only;
    independent_failure.constraint_id = "independent-failure";
    independent_failure.upper_bound = 4.0;
    independent_failure.or_group_id.clear();
    result = engine.checkConstraints(
        {or_upper, aggregate_alternative, independent_failure}, or_window);
    assert(!result.satisfied);
    assert(result.violations.size() == 2);
    for (const auto& violation : result.violations) {
        assert(violation.constraint_id == "independent-failure");
    }

    std::cout << "constraint_check_engine_test passed\n";
    return 0;
}
