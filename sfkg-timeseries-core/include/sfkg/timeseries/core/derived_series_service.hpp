#pragma once

#include <cstdint>
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
    // Independent derived configurations are computed in a bounded parallel
    // phase and published in a serialized phase. Derived results are written
    // to WindowService only.
    OperationResult refresh(const ProjectId& project_id);
    OperationResult refresh();

    // Recomputes only derived sequences affected by an append-only window
    // update when it is safe to do so. Out-of-order updates and unsupported
    // source kinds fall back to a full refresh for correctness. Window
    // eviction is handled as an incremental prefix removal plus patch.
    OperationResult refresh(const WindowUpdateResult& update);
    OperationResult refresh(
        const ProjectId& project_id,
        const WindowUpdateResult& update);

private:
    OperationResult refreshInternal(
        const ProjectId& project_id,
        const WindowUpdateResult* update);

    const RuntimeConfigRegistry& configs_;
    WindowService& window_service_;
    // Full refreshes (configuration synchronization) are serialized with one
    // another. Incremental ingest refreshes may run concurrently; the
    // generation in WindowUpdateResult prevents an older computation from
    // publishing after a newer one. WindowService protects each derived
    // sequence while it is patched.
    mutable std::mutex full_refresh_mutex_;
    mutable std::mutex publish_mutex_;
    std::uint64_t last_published_generation_{0};
};

}  // namespace sfkg::timeseries::core
