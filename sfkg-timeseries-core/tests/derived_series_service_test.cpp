#include <cassert>
#include <cmath>
#include <iostream>
#include <memory>

#include "sfkg/timeseries/core/derived_series_service.hpp"

using namespace sfkg::timeseries::core;

namespace {

RuntimeInstanceConfig instance(
    const SequenceId& sequence_id,
    SeriesKind kind = SeriesKind::Continuous) {
    RuntimeInstanceConfig result;
    result.sequence_id = sequence_id;
    result.data_source_id = "test-source";
    result.external_sequence_id = sequence_id;
    result.data_type = "double";
    result.series_kind = kind;
    return result;
}

double valueAt(const WindowData& data, const SequenceId& sequence_id,
               Timestamp time) {
    const auto sequence = data.sequence_values.find(sequence_id);
    assert(sequence != data.sequence_values.end());
    for (const auto& point : sequence->second) {
        if (point.time == time) {
            const auto value = std::get_if<double>(&point.value);
            assert(value != nullptr);
            return *value;
        }
    }
    assert(false);
    return 0.0;
}

}  // namespace

int main() {
    RuntimeConfigRegistry registry;
    assert(registry.replaceInstanceConfigs({{
        instance("a"), instance("b"), instance("state", SeriesKind::Discrete)
    }}).code == OperationCode::Ok);

    WindowService window;
    assert(window.configureWindowSize(1'000).code == OperationCode::Ok);
    assert(window.buildTimeWindow({{
        {0, "a", 0.0},
        {10, "a", 10.0},
        {0, "b", 2.0},
        {5, "b", 4.0},
        {10, "b", 2.0},
    }}).code == OperationCode::Ok);

    RuntimeDerivedSeriesConfig linear;
    linear.derived_sequence_id = "linear-output";
    linear.enabled = true;
    linear.formula = DerivedLinearCombination{
        {{"a", 2.0}, {"b", 3.0}}, 1.0};
    RuntimeDerivedSeriesConfig second_linear;
    second_linear.derived_sequence_id = "linear-output-2";
    second_linear.enabled = true;
    second_linear.formula = DerivedLinearCombination{
        {{"a", 1.0}, {"b", -1.0}}, 0.0};
    assert(registry.upsertDerivedSeriesConfigs({{linear, second_linear}}).code ==
           OperationCode::Ok);

    DerivedSeriesService derived(registry, window);
    auto result = derived.refresh();
    assert(result.code == OperationCode::Ok);
    auto queried = window.queryWindowData({{"linear-output"}, std::nullopt,
                                            std::nullopt});
    assert(queried.operation.code == OperationCode::Ok);
    assert(std::abs(valueAt(queried.data, "linear-output", 0) - 7.0) < 1e-9);
    // At t=5, a is linearly interpolated to 5: 2*5 + 3*4 + 1 = 23.
    assert(std::abs(valueAt(queried.data, "linear-output", 5) - 23.0) < 1e-9);
    assert(std::abs(valueAt(queried.data, "linear-output", 10) - 27.0) < 1e-9);
    queried = window.queryWindowData({{"linear-output-2"}, std::nullopt,
                                      std::nullopt});
    assert(std::abs(valueAt(queried.data, "linear-output-2", 5) - 1.0) <
           1e-9);

    const auto append_update = window.buildTimeWindowIncremental({{
        {20, "a", 20.0},
        {20, "b", 4.0},
    }});
    assert(append_update.operation.code == OperationCode::Ok);
    assert(append_update.incremental_safe);
    result = derived.refresh(append_update);
    assert(result.code == OperationCode::Ok);
    queried = window.queryWindowData({{"linear-output"}, std::nullopt,
                                      std::nullopt});
    assert(std::abs(valueAt(queried.data, "linear-output", 20) - 53.0) <
           1e-9);
    queried = window.queryWindowData({{"linear-output-2"}, std::nullopt,
                                      std::nullopt});
    assert(std::abs(valueAt(queried.data, "linear-output-2", 20) - 16.0) <
           1e-9);

    const auto out_of_order_update = window.buildTimeWindowIncremental({{
        {5, "a", 20.0},
    }});
    assert(out_of_order_update.operation.code == OperationCode::Ok);
    assert(!out_of_order_update.incremental_safe);
    result = derived.refresh(out_of_order_update);
    assert(result.code == OperationCode::Ok);
    queried = window.queryWindowData({{"linear-output"}, std::nullopt,
                                      std::nullopt});
    assert(std::abs(valueAt(queried.data, "linear-output", 5) - 53.0) <
           1e-9);

    auto left = std::make_shared<DerivedExpression>();
    left->kind = DerivedExpression::NodeKind::Sequence;
    left->sequence_id = "a";
    auto right = std::make_shared<DerivedExpression>();
    right->kind = DerivedExpression::NodeKind::Sequence;
    right->sequence_id = "b";
    RuntimeDerivedSeriesConfig expression;
    expression.derived_sequence_id = "expression-output";
    expression.enabled = true;
    expression.formula = DerivedExpression{
        DerivedExpression::NodeKind::Binary,
        {},
        0.0,
        {DerivedOperator::Multiply, left, right}};
    assert(registry.upsertDerivedSeriesConfigs({{expression}}).code ==
           OperationCode::Ok);
    result = derived.refresh();
    assert(result.code == OperationCode::Ok);
    queried = window.queryWindowData({{"expression-output"}, std::nullopt,
                                      std::nullopt});
    assert(std::abs(valueAt(queried.data, "expression-output", 5) - 80.0) <
           1e-9);

    RuntimeDerivedSeriesConfig invalid;
    invalid.derived_sequence_id = "invalid-output";
    invalid.enabled = true;
    invalid.formula = DerivedLinearCombination{{{"state", 1.0}}, 0.0};
    result = registry.upsertDerivedSeriesConfigs({{invalid}});
    assert(result.code == OperationCode::InvalidArgument);

    expression.enabled = false;
    assert(registry.upsertDerivedSeriesConfigs({{expression}}).code ==
           OperationCode::Ok);
    result = derived.refresh();
    assert(result.code == OperationCode::Ok);
    queried = window.queryWindowData({{"expression-output"}, std::nullopt,
                                      std::nullopt});
    assert(queried.data.sequence_values.empty());

    std::cout << "derived_series_service_test passed\n";
    return 0;
}
