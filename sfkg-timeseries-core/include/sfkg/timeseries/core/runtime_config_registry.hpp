#pragma once

#include <shared_mutex>

#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core {

struct ConstraintLookupResult {
    std::vector<ConstraintRule> enabled_rules;
    std::vector<std::string> missing_ids;
    std::vector<std::string> disabled_ids;
};

// Holds validated runtime copies only; configuration persistence and
// lifecycle management remain in the unified service.
class RuntimeConfigRegistry {
public:
    OperationResult replaceInstanceConfigs(
        const RuntimeConfigSnapshot<RuntimeInstanceConfig>& snapshot);

    // Incrementally inserts or updates the supplied instances and preserves
    // all instances that are not present in the request.
    OperationResult upsertInstanceConfigs(
        const RuntimeConfigSnapshot<RuntimeInstanceConfig>& snapshot);

    // Validates rule structure, variable mappings and referenced numeric
    // sequences before atomically replacing the constraint index.
    OperationResult replaceConstraints(
        const RuntimeConfigSnapshot<RuntimeConstraintConfig>& snapshot);

    // Incrementally inserts or updates constraints by constraint_id.
    OperationResult upsertConstraints(
        const RuntimeConfigSnapshot<RuntimeConstraintConfig>& snapshot);

    OperationResult replaceRelations(
        const RuntimeConfigSnapshot<RuntimeRelationConfig>& snapshot);

    // Incrementally inserts or updates relations by relation_id.
    OperationResult upsertRelations(
        const RuntimeConfigSnapshot<RuntimeRelationConfig>& snapshot);

    std::optional<RuntimeInstanceConfig> findInstance(
        const SequenceId& sequence_id) const;
    std::optional<SequenceId> resolveSequenceId(
        const std::string& data_source_id,
        const std::string& external_sequence_id) const;
    std::optional<RuntimeRelationConfig> findRelation(
        const std::string& relation_id) const;
    ConstraintLookupResult lookupConstraints(
        const std::vector<std::string>& constraint_ids) const;
    std::vector<ConstraintRule> enabledConstraints(
        const std::vector<std::string>& constraint_ids) const;

    // Returns a copied, read-only snapshot of every currently enabled rule.
    // The copy lets continuous ingest checks run without holding the
    // registry's shared lock while the constraint engine evaluates data.
    std::vector<ConstraintRule> allEnabledConstraints() const;

private:
    // Readers may run concurrently; snapshot replacement takes the exclusive
    // lock so all related indexes become visible atomically.
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
