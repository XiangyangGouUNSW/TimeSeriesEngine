#include <chrono>
#include <cstdint>
#include <iostream>
#include <vector>

#include "sfkg/timeseries/core/constraint_check_engine.hpp"
#include "sfkg/timeseries/core/window_service.hpp"

namespace core = sfkg::timeseries::core;

int main() {
    constexpr std::size_t kBatchCount = 1'000;
    constexpr std::size_t kPointsPerBatch = 1'000;
    constexpr std::size_t kRuleCount = 100;

    core::WindowService window;
    if (window.configureWindowSize(2'000'000).code !=
        core::OperationCode::Ok) {
        return 1;
    }
    std::vector<core::ConstraintRule> rules;
    rules.reserve(kRuleCount);
    for (std::size_t index = 0; index < kRuleCount; ++index) {
        core::ConstraintRule rule;
        rule.constraint_id = "aggregate-" + std::to_string(index);
        rule.variable_mapping = {{"x", "sequence"}};
        rule.upper_bound = 2'000'000.0;
        rule.terms = {
            {"x", 1.0, 0, core::ConstraintAggregation::Average},
            {"x", 0.1, 0, core::ConstraintAggregation::Maximum},
            {"x", 0.1, 0, core::ConstraintAggregation::Minimum}};
        rules.push_back(std::move(rule));
    }

    core::ConstraintCheckEngine engine;
    const auto started = std::chrono::steady_clock::now();
    core::Timestamp next_time = 0;
    for (std::size_t batch_index = 0;
         batch_index < kBatchCount;
         ++batch_index) {
        core::TimeseriesBatch batch;
        batch.points.reserve(kPointsPerBatch);
        for (std::size_t point_index = 0;
             point_index < kPointsPerBatch;
             ++point_index) {
            batch.points.push_back({
                next_time,
                "sequence",
                static_cast<double>(next_time)});
            ++next_time;
        }
        const auto updated = window.buildTimeWindowIncremental(batch);
        if (updated.operation.code != core::OperationCode::Ok ||
            !updated.incremental_safe) {
            return 2;
        }
        const auto statistics =
            window.queryWindowStatistics({"sequence"});
        if (statistics.operation.code != core::OperationCode::Ok) {
            return 3;
        }
        const auto checked = engine.checkConstraints(rules, statistics.data);
        if (checked.operation.code != core::OperationCode::Ok ||
            checked.evaluated_count != kRuleCount) {
            return 4;
        }
    }
    const auto elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - started).count();
    const auto total_points = kBatchCount * kPointsPerBatch;
    std::cout << "aggregate_incremental_stress points=" << total_points
              << " rules_per_batch=" << kRuleCount
              << " elapsed_seconds=" << elapsed
              << " points_per_second="
              << static_cast<double>(total_points) / elapsed << '\n';
    return 0;
}
