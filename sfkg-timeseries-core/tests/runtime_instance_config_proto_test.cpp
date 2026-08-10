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
    return 0;
}
