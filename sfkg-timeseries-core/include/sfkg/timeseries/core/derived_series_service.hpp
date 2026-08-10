#pragma once

#include <mutex>

#include "sfkg/timeseries/core/runtime_config_registry.hpp"
#include "sfkg/timeseries/core/window_service.hpp"

namespace sfkg::timeseries::core {

class DerivedSeriesService {
public:
    DerivedSeriesService(
        const RuntimeConfigRegistry& configs,
        WindowService& window_service)
        : configs_(configs), window_service_(window_service) {}

    // Recomputes enabled derived sequences from the current hot window.
    // Derived results are written to WindowService only.
    OperationResult refresh();

private:
    const RuntimeConfigRegistry& configs_;
    WindowService& window_service_;
    // A refresh reads a window snapshot and then replaces derived sequences.
    // Serializing refreshes prevents concurrent ingest RPCs from publishing
    // derived configurations in an inconsistent order; WindowService itself
    // remains independently safe for concurrent raw reads and writes.
    mutable std::mutex refresh_mutex_;
};

}  // namespace sfkg::timeseries::core
