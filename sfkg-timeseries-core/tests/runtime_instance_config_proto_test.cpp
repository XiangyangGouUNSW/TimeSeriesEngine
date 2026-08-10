#include <cassert>
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

    core::RuntimeInstanceConfig target;
    std::string error;
    assert(conversion::fromProto(source, &target, &error));
    assert(error.empty());
    assert(target.data_type == "double");
    assert(target.series_kind == core::SeriesKind::Continuous);

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
    core::RuntimeWindowConfig window_target;
    error.clear();
    assert(conversion::fromProto(window_source, &window_target, &error));
    assert(error.empty());
    assert(window_target.window_size == 259'200'000);

    proto::DerivedSeriesConfig derived_source;
    derived_source.set_derived_sequence_id("temperature-pressure-sum");
    derived_source.set_enabled(true);
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
