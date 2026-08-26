#include <cassert>
#include <optional>
#include <string>

#include "grpc/internal/proto_conversion.hpp"

namespace core = sfkg::timeseries::core;
namespace conversion = sfkg::timeseries::core::grpc::conversion;
namespace proto = sfkg::timeseries::core::v1;

int main() {
    proto::RuntimeInstanceConfig source;
    source.set_sequence_id("temperature-1");
    source.set_data_source_id("source-a");
    source.set_external_sequence_id("temp");
    source.set_category_id("temperature");
    source.set_data_type("double");
    source.set_series_kind(proto::SERIES_KIND_CONTINUOUS);
    source.set_project_id("project-a");

    core::RuntimeInstanceConfig target;
    std::string error;
    assert(conversion::fromProto(source, &target, &error));
    assert(error.empty());
    assert(target.data_type == "double");
    assert(target.series_kind == core::SeriesKind::Continuous);
    assert(target.project_id == "project-a");

    proto::RuntimeInstanceConfig legacy;
    legacy.set_sequence_id("state-1");
    legacy.set_data_source_id("source-a");
    legacy.set_external_sequence_id("state");
    legacy.set_data_type("bool");
    target = {};
    error.clear();
    assert(conversion::fromProto(legacy, &target, &error));
    assert(target.series_kind == core::SeriesKind::Unspecified);

    source.set_series_kind(static_cast<proto::SeriesKind>(99));
    error.clear();
    assert(!conversion::fromProto(source, &target, &error));
    assert(error == "unknown series_kind");

    proto::RuntimeWindowConfig window_source;
    window_source.set_window_size(259'200'000);
    window_source.set_project_id("project-a");
    core::RuntimeWindowConfig window_target;
    error.clear();
    assert(conversion::fromProto(window_source, &window_target, &error));
    assert(error.empty());
    assert(window_target.window_size == 259'200'000);
    assert(window_target.project_id == "project-a");

    proto::RuntimeConstraintConfig constraint_source;
    constraint_source.set_enabled(true);
    constraint_source.set_project_id("project-a");
    auto* constraint_rule = constraint_source.mutable_rule();
    constraint_rule->set_constraint_id("average-upper");
    (*constraint_rule->mutable_variable_mapping())["x"] = "temperature-1";
    constraint_rule->set_upper_bound(30.0);
    constraint_rule->set_or_group_id("temperature-choice");
    auto* constraint_term = constraint_rule->add_terms();
    constraint_term->set_variable("x");
    constraint_term->set_coefficient(1.0);
    constraint_term->set_aggregation(
        proto::CONSTRAINT_AGGREGATION_AVERAGE);
    core::RuntimeConstraintConfig constraint_target;
    error.clear();
    assert(conversion::fromProto(
        constraint_source, &constraint_target, &error));
    assert(!constraint_target.rule.lower_bound);
    assert(constraint_target.rule.upper_bound ==
           std::optional<double>{30.0});
    assert(constraint_target.rule.or_group_id == "temperature-choice");
    assert(constraint_target.rule.terms.front().aggregation ==
           core::ConstraintAggregation::Average);

    core::ConstraintCheckResult check_result;
    check_result.operation.code = core::OperationCode::Ok;
    check_result.violations.push_back({
        "average-upper", 10, std::nullopt, 30.0, 31.0,
        {{"x", "temperature-1", 1.0, 0, 10, 31.0,
          core::ConstraintAggregation::Average}},
        "temperature-choice"});
    proto::CheckConstraintsResponse check_response;
    conversion::toProto(check_result, &check_response);
    assert(check_response.violations_size() == 1);
    assert(!check_response.violations(0).has_lower_bound());
    assert(check_response.violations(0).has_upper_bound());
    assert(check_response.violations(0).or_group_id() ==
           "temperature-choice");
    assert(check_response.violations(0).term_values(0).aggregation() ==
           proto::CONSTRAINT_AGGREGATION_AVERAGE);

    proto::DerivedSeriesConfig derived_source;
    derived_source.set_derived_sequence_id("temperature-pressure-sum");
    derived_source.set_enabled(true);
    derived_source.set_project_id("project-a");
    auto* linear = derived_source.mutable_linear_combination();
    linear->set_bias(1.5);
    auto* first_term = linear->add_terms();
    first_term->set_sequence_id("temperature-1");
    first_term->set_coefficient(2.0);
    core::RuntimeDerivedSeriesConfig derived_target;
    error.clear();
    assert(conversion::fromProto(derived_source, &derived_target, &error));
    assert(error.empty());
    const auto* linear_target =
        std::get_if<core::DerivedLinearCombination>(&derived_target.formula);
    assert(linear_target != nullptr);
    assert(linear_target->terms.size() == 1);
    assert(linear_target->terms.front().sequence_id == "temperature-1");
    assert(linear_target->bias == 1.5);
    assert(derived_target.project_id == "project-a");

    proto::DerivedSeriesConfig expression_source;
    expression_source.set_derived_sequence_id("temperature-double");
    auto* expression = expression_source.mutable_expression();
    expression->set_sequence_id("temperature-1");
    derived_target = {};
    error.clear();
    assert(conversion::fromProto(expression_source, &derived_target, &error));
    const auto* expression_target =
        std::get_if<core::DerivedExpression>(&derived_target.formula);
    assert(expression_target != nullptr);
    assert(expression_target->kind ==
           core::DerivedExpression::NodeKind::Sequence);
    assert(expression_target->sequence_id == "temperature-1");

    expression_source.mutable_expression()->clear_node();
    error.clear();
    assert(!conversion::fromProto(expression_source, &derived_target, &error));
    assert(error == "derived expression node is not set");
    return 0;
}
