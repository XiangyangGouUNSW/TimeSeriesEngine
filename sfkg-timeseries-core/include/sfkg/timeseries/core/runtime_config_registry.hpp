#pragma once

#include <shared_mutex>

#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core {

// Holds validated runtime copies only; configuration persistence and
// lifecycle management remain in the unified service.
class RuntimeConfigRegistry {
public:
    OperationResult replaceInstanceConfigs(
        const RuntimeConfigSnapshot<RuntimeInstanceConfig>& snapshot);

    // Validates rule structure, variable mappings and referenced numeric
    // sequences before atomically replacing the constraint index.
    OperationResult replaceConstraints(
        const RuntimeConfigSnapshot<RuntimeConstraintConfig>& snapshot);

    OperationResult replaceRelations(
        const RuntimeConfigSnapshot<RuntimeRelationConfig>& snapshot);

    std::optional<RuntimeInstanceConfig> findInstance(
        const SequenceId& sequence_id) const;
    std::optional<SequenceId> resolveSequenceId(
        const std::string& data_source_id,
        const std::string& external_sequence_id) const;
    std::vector<ConstraintRule> enabledConstraints(
        const std::vector<std::string>& constraint_ids) const;

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<SequenceId, RuntimeInstanceConfig>
        instance_configs_;
    std::unordered_map<std::string, SequenceId>
        external_sequence_index_;
    std::unordered_map<std::string, RuntimeConstraintConfig>
        constraints_;
    std::unordered_map<std::string, RuntimeRelationConfig>
        relations_;
};

}  // namespace sfkg::timeseries::core
