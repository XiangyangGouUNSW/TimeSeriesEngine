#include <cassert>
#include <cstdint>
#include <iostream>
#include <variant>
#include <utility>

#include "sfkg/timeseries/core/alignment_service.hpp"
#include "sfkg/timeseries/core/runtime_config_registry.hpp"

using namespace sfkg::timeseries::core;

namespace {

double asDouble(const TimeseriesValue& value) {
    if (const auto* number = std::get_if<double>(&value)) {
        return *number;
    }
    if (const auto* number = std::get_if<std::int64_t>(&value)) {
        return static_cast<double>(*number);
    }
    assert(false && "test value is not numeric");
    return 0.0;
}

const std::string& asString(const TimeseriesValue& value) {
    const auto* string_value = std::get_if<std::string>(&value);
    assert(string_value != nullptr && "test value is not a string");
    return *string_value;
}

SequenceAlignmentConfig sequence(
    SequenceId id,
    BucketAggregation aggregation = BucketAggregation::Average,
    GapFillMethod fill = GapFillMethod::Linear) {
    return SequenceAlignmentConfig{
        std::move(id), VariableRole::Independent, aggregation, fill};
}

RuntimeRelationConfig fixedRelation(
    SequenceId target,
    SequenceId source,
    std::int64_t lag) {
    RuntimeRelationConfig relation;
    relation.relation_id = "relation-1";
    relation.target_sequence_id = std::move(target);
    relation.enabled = true;
    relation.sources.push_back(
        RuntimeRelationSource{std::move(source), 1.0, lag});
    return relation;
}

}  // namespace

int main() {
    RuntimeConfigRegistry registry;
    RuntimeInstanceConfig temperature_instance;
    temperature_instance.sequence_id = "temperature-1";
    temperature_instance.data_source_id = "test";
    temperature_instance.external_sequence_id = "temperature-1";
    temperature_instance.series_kind = SeriesKind::Continuous;
    assert(registry.replaceInstanceConfigs(
        RuntimeConfigSnapshot<RuntimeInstanceConfig>{{temperature_instance}})
               .code == OperationCode::Ok);

    AlignmentService service(registry);

    AlignmentConfig basic_config;
    basic_config.bucket_interval = 5;
    basic_config.sequences.push_back(sequence("temperature-1"));

    WindowData basic_window;
    basic_window.window_start_time = 0;
    basic_window.window_end_time = 20;
    basic_window.sequence_values["temperature-1"] = {
        {0, "temperature-1", 0.0},
        {10, "temperature-1", 20.0}};

    auto result = service.alignWindowData(basic_window, basic_config, {});
    assert(result.operation.code == OperationCode::Ok);
    assert(result.aligned_data.samples.size() == 4);
    assert(asDouble(result.aligned_data.samples[0].values.at("temperature-1")) == 0.0);
    assert(asDouble(result.aligned_data.samples[1].values.at("temperature-1")) == 10.0);
    assert(asDouble(result.aligned_data.samples[2].values.at("temperature-1")) == 20.0);
    // Linear interpolation is used only between known points; boundaries use
    // NEAR, so the last bucket is filled from the previous known value.
    assert(asDouble(result.aligned_data.samples[3].values.at("temperature-1")) == 20.0);

    // With no AlignmentConfig, the continuous sequence uses the registry
    // default Average + Linear and the smallest positive gap as interval.
    WindowData inferred_window;
    inferred_window.window_start_time = 0;
    inferred_window.window_end_time = 4;
    inferred_window.sequence_values["temperature-1"] = {
        {0, "temperature-1", 0.0},
        {1, "temperature-1", 10.0},
        {3, "temperature-1", 30.0}};
    result = service.alignWindowData(inferred_window);
    assert(result.operation.code == OperationCode::Ok);
    assert(result.aligned_data.samples.size() == 4);
    assert(asDouble(result.aligned_data.samples[2].values.at("temperature-1")) == 20.0);

    RuntimeInstanceConfig status_instance;
    status_instance.sequence_id = "machine-status-1";
    status_instance.data_source_id = "test";
    status_instance.external_sequence_id = "machine-status-1";
    status_instance.series_kind = SeriesKind::Discrete;
    assert(registry.upsertInstanceConfigs(
        RuntimeConfigSnapshot<RuntimeInstanceConfig>{{status_instance}})
               .code == OperationCode::Ok);
    AlignmentConfig status_config;
    status_config.bucket_interval = 2;
    status_config.sequences.push_back({"machine-status-1"});
    WindowData status_window;
    status_window.window_start_time = 0;
    status_window.window_end_time = 8;
    status_window.sequence_values["machine-status-1"] = {
        {0, "machine-status-1", std::string("off")},
        {6, "machine-status-1", std::string("on")}};
    result = service.alignWindowData(status_window, status_config);
    assert(result.operation.code == OperationCode::Ok);
    assert(asString(result.aligned_data.samples[1].values.at("machine-status-1")) == "off");
    assert(asString(result.aligned_data.samples[2].values.at("machine-status-1")) == "off");
    assert(asString(result.aligned_data.samples[3].values.at("machine-status-1")) == "on");

    auto near_config = basic_config;
    near_config.sequences.front().fill_method = GapFillMethod::Near;
    basic_window.sequence_values["temperature-1"] = {
        {0, "temperature-1", 0.0},
        {15, "temperature-1", 15.0}};
    result = service.alignWindowData(basic_window, near_config, {});
    assert(result.operation.code == OperationCode::Ok);
    assert(asDouble(result.aligned_data.samples[1].values.at("temperature-1")) == 0.0);
    assert(asDouble(result.aligned_data.samples[2].values.at("temperature-1")) == 15.0);

    AlignmentConfig relation_config;
    relation_config.bucket_interval = 5;
    relation_config.sequences = {
        sequence("quality-1", BucketAggregation::First, GapFillMethod::Near),
        sequence("temperature-1", BucketAggregation::Average, GapFillMethod::Near)};

    WindowData relation_window;
    relation_window.window_start_time = 10;
    relation_window.window_end_time = 20;
    relation_window.sequence_values["quality-1"] = {
        {10, "quality-1", 100.0},
        {15, "quality-1", 101.0}};
    // lag=3 maps source time 7/8 into the target bucket [10, 15), and
    // source time 12 into [15, 20).
    relation_window.sequence_values["temperature-1"] = {
        {7, "temperature-1", 70.0},
        {8, "temperature-1", 80.0},
        {12, "temperature-1", 120.0}};

    result = service.alignWindowData(
        relation_window, relation_config,
        {fixedRelation("quality-1", "temperature-1", 3)});
    assert(result.operation.code == OperationCode::Ok);
    assert(result.aligned_data.samples.size() == 2);
    assert(asDouble(result.aligned_data.samples[0].values.at("temperature-1")) == 75.0);
    assert(asDouble(result.aligned_data.samples[1].values.at("temperature-1")) == 120.0);

    RuntimeRelationConfig range_relation = fixedRelation(
        "quality-1", "temperature-1", 3);
    range_relation.sources.front().lag = RelationLagRange{2, 4};
    result = service.alignWindowData(
        relation_window, relation_config, {range_relation});
    assert(result.operation.code == OperationCode::NotImplemented);

    RuntimeRelationConfig no_lag_relation = fixedRelation(
        "quality-1", "temperature-1", 3);
    no_lag_relation.sources.front().lag = std::monostate{};
    result = service.alignWindowData(
        relation_window, relation_config, {no_lag_relation});
    assert(result.operation.code == OperationCode::NotImplemented);

    std::cout << "alignment_service_test passed\n";
    return 0;
}
