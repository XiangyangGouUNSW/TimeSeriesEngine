#include "sfkg/timeseries/core/runtime_config_registry.hpp"

#include <algorithm>
#include <cmath>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <utility>

#include "operation_helpers.hpp"

namespace sfkg::timeseries::core {
namespace {

std::string externalKey(
    const std::string& data_source_id,
    const std::string& external_sequence_id) {
    return data_source_id + '\x1f' + external_sequence_id;
}

bool isBlank(const std::string& value) {
    return value.empty();
}

bool validSeriesKind(SeriesKind kind) {
    switch (kind) {
        case SeriesKind::Unspecified:
        case SeriesKind::Continuous:
        case SeriesKind::Discrete:
        case SeriesKind::Categorical:
            return true;
    }
    return false;
}

}  // namespace

OperationResult RuntimeConfigRegistry::replaceInstanceConfigs(
    const RuntimeConfigSnapshot<RuntimeInstanceConfig>& snapshot) {
    std::unordered_map<SequenceId, RuntimeInstanceConfig> instances;
    std::unordered_map<std::string, SequenceId> external_index;

    for (const auto& item : snapshot.items) {
        if (isBlank(item.sequence_id) || isBlank(item.data_source_id) ||
            isBlank(item.external_sequence_id)) {
            return internal::invalidArgument(
                "instance identifiers must not be empty");
        }
        if (!validSeriesKind(item.series_kind)) {
            return internal::invalidArgument(
                "instance series_kind is invalid");
        }
        if (!instances.emplace(item.sequence_id, item).second) {
            return internal::invalidArgument(
                "duplicate sequence_id: " + item.sequence_id);
        }
        const auto key = externalKey(
            item.data_source_id, item.external_sequence_id);
        if (!external_index.emplace(key, item.sequence_id).second) {
            return internal::invalidArgument(
                "duplicate external sequence identifier");
        }
    }

    const auto count = instances.size();
    {
        // Build and validate the replacement indexes before taking this lock;
        // only the short publication step needs exclusive access.
        std::unique_lock lock(mutex_);
        instance_configs_.swap(instances);
        external_sequence_index_.swap(external_index);
    }
    return internal::ok(count, "instance configuration snapshot replaced");
}

OperationResult RuntimeConfigRegistry::upsertInstanceConfigs(
    const RuntimeConfigSnapshot<RuntimeInstanceConfig>& snapshot) {
    std::unique_lock lock(mutex_);
    auto instances = instance_configs_;
    auto external_index = external_sequence_index_;
    std::unordered_set<SequenceId> seen_sequence_ids;

    for (const auto& item : snapshot.items) {
        if (isBlank(item.sequence_id) || isBlank(item.data_source_id) ||
            isBlank(item.external_sequence_id)) {
            return internal::invalidArgument(
                "instance identifiers must not be empty");
        }
        if (!validSeriesKind(item.series_kind)) {
            return internal::invalidArgument(
                "instance series_kind is invalid");
        }
        if (!seen_sequence_ids.emplace(item.sequence_id).second) {
            return internal::invalidArgument(
                "duplicate sequence_id in incremental update: " +
                item.sequence_id);
        }

        const auto existing = instances.find(item.sequence_id);
        if (existing != instances.end()) {
            const auto old_key = externalKey(
                existing->second.data_source_id,
                existing->second.external_sequence_id);
            const auto old_external = external_index.find(old_key);
            if (old_external != external_index.end() &&
                old_external->second == item.sequence_id) {
                external_index.erase(old_external);
            }
        }

        const auto new_key = externalKey(
            item.data_source_id, item.external_sequence_id);
        const auto conflicting = external_index.find(new_key);
        if (conflicting != external_index.end() &&
            conflicting->second != item.sequence_id) {
            return internal::invalidArgument(
                "duplicate external sequence identifier");
        }
        instances[item.sequence_id] = item;
        external_index[new_key] = item.sequence_id;
    }

    const auto count = snapshot.items.size();
    instance_configs_.swap(instances);
    external_sequence_index_.swap(external_index);
    return internal::ok(count, "instance configuration incrementally synchronized");
}

OperationResult RuntimeConfigRegistry::replaceConstraints(
    const RuntimeConfigSnapshot<RuntimeConstraintConfig>& snapshot) {
    // Constraint validation below reads the current instance index, so keep
    // the snapshot replacement atomic with respect to readers.
    std::unique_lock lock(mutex_);
    std::unordered_map<std::string, RuntimeConstraintConfig> constraints;

    for (const auto& item : snapshot.items) {
        const auto& rule = item.rule;
        if (isBlank(rule.constraint_id)) {
            return internal::invalidArgument("constraint_id must not be empty");
        }
        if (!std::isfinite(rule.lower_bound) ||
            !std::isfinite(rule.upper_bound) ||
            rule.lower_bound > rule.upper_bound) {
            return internal::invalidArgument(
                "constraint bounds must be finite and ordered");
        }
        if (rule.variable_mapping.empty() || rule.terms.empty()) {
            return internal::invalidArgument(
                "constraint mappings and terms must not be empty");
        }

        std::size_t previous_offset = 0;
        bool first_term = true;
        for (const auto& term : rule.terms) {
            if (isBlank(term.variable) || !std::isfinite(term.coefficient)) {
                return internal::invalidArgument(
                    "constraint terms must contain a variable and finite coefficient");
            }
            if (rule.variable_mapping.find(term.variable) ==
                rule.variable_mapping.end()) {
                return internal::invalidArgument(
                    "constraint term has no variable mapping: " + term.variable);
            }
            if ((first_term && term.sample_offset != 0) ||
                (!first_term && term.sample_offset < previous_offset)) {
                return internal::invalidArgument(
                    "constraint offsets must start at zero and be nondecreasing");
            }
            first_term = false;
            previous_offset = term.sample_offset;
        }

        for (const auto& [variable, sequence_id] : rule.variable_mapping) {
            if (isBlank(variable) ||
                instance_configs_.find(sequence_id) == instance_configs_.end()) {
                return internal::invalidArgument(
                    "constraint references an unknown sequence: " + sequence_id);
            }
        }

        if (!constraints.emplace(rule.constraint_id, item).second) {
            return internal::invalidArgument(
                "duplicate constraint_id: " + rule.constraint_id);
        }
    }

    const auto count = constraints.size();
    constraints_.swap(constraints);
    return internal::ok(count, "constraint configuration snapshot replaced");
}

OperationResult RuntimeConfigRegistry::upsertConstraints(
    const RuntimeConfigSnapshot<RuntimeConstraintConfig>& snapshot) {
    std::unique_lock lock(mutex_);
    auto constraints = constraints_;
    std::unordered_set<std::string> seen_constraint_ids;

    for (const auto& item : snapshot.items) {
        const auto& rule = item.rule;
        if (isBlank(rule.constraint_id)) {
            return internal::invalidArgument("constraint_id must not be empty");
        }
        if (!seen_constraint_ids.emplace(rule.constraint_id).second) {
            return internal::invalidArgument(
                "duplicate constraint_id in incremental update: " +
                rule.constraint_id);
        }
        if (!std::isfinite(rule.lower_bound) ||
            !std::isfinite(rule.upper_bound) ||
            rule.lower_bound > rule.upper_bound) {
            return internal::invalidArgument(
                "constraint bounds must be finite and ordered");
        }
        if (rule.variable_mapping.empty() || rule.terms.empty()) {
            return internal::invalidArgument(
                "constraint mappings and terms must not be empty");
        }

        std::size_t previous_offset = 0;
        bool first_term = true;
        for (const auto& term : rule.terms) {
            if (isBlank(term.variable) || !std::isfinite(term.coefficient)) {
                return internal::invalidArgument(
                    "constraint terms must contain a variable and finite coefficient");
            }
            if (rule.variable_mapping.find(term.variable) ==
                rule.variable_mapping.end()) {
                return internal::invalidArgument(
                    "constraint term has no variable mapping: " + term.variable);
            }
            if ((first_term && term.sample_offset != 0) ||
                (!first_term && term.sample_offset < previous_offset)) {
                return internal::invalidArgument(
                    "constraint offsets must start at zero and be nondecreasing");
            }
            first_term = false;
            previous_offset = term.sample_offset;
        }

        for (const auto& [variable, sequence_id] : rule.variable_mapping) {
            if (isBlank(variable) ||
                instance_configs_.find(sequence_id) == instance_configs_.end()) {
                return internal::invalidArgument(
                    "constraint references an unknown sequence: " + sequence_id);
            }
        }
        constraints[rule.constraint_id] = item;
    }

    const auto count = snapshot.items.size();
    constraints_.swap(constraints);
    return internal::ok(count, "constraint configuration incrementally synchronized");
}

OperationResult RuntimeConfigRegistry::replaceRelations(
    const RuntimeConfigSnapshot<RuntimeRelationConfig>& snapshot) {
    std::unordered_map<std::string, RuntimeRelationConfig> relations;
    for (const auto& item : snapshot.items) {
        if (isBlank(item.relation_id) || isBlank(item.target_sequence_id) ||
            item.sources.empty()) {
            return internal::invalidArgument(
                "relation_id and target_sequence_id must not be empty, "
                "and relation sources must not be empty");
        }
        if (!std::isfinite(item.confidence)) {
            return internal::invalidArgument(
                "relation confidence is invalid");
        }
        std::unordered_map<std::string, bool> source_sequences;
        for (const auto& source : item.sources) {
            if (isBlank(source.source_sequence_id) ||
                !std::isfinite(source.weight)) {
                return internal::invalidArgument(
                    "relation source sequence and weight must be valid");
            }
            if (!source_sequences.emplace(
                    source.source_sequence_id, true).second) {
                return internal::invalidArgument(
                    "duplicate relation source sequence: " +
                    source.source_sequence_id);
            }
            if (const auto* range = std::get_if<RelationLagRange>(
                    &source.lag);
                range != nullptr && range->min > range->max) {
                return internal::invalidArgument(
                    "relation source lag range is invalid");
            }
        }
        if (!relations.emplace(item.relation_id, item).second) {
            return internal::invalidArgument(
                "duplicate relation_id: " + item.relation_id);
        }
    }

    const auto count = relations.size();
    {
        // Publish the fully validated relation snapshot as one replacement.
        std::unique_lock lock(mutex_);
        relations_.swap(relations);
    }
    return internal::ok(count, "relation configuration snapshot replaced");
}

OperationResult RuntimeConfigRegistry::upsertRelations(
    const RuntimeConfigSnapshot<RuntimeRelationConfig>& snapshot) {
    std::unique_lock lock(mutex_);
    auto relations = relations_;
    std::unordered_set<std::string> seen_relation_ids;

    for (const auto& item : snapshot.items) {
        if (isBlank(item.relation_id) || isBlank(item.target_sequence_id) ||
            item.sources.empty()) {
            return internal::invalidArgument(
                "relation_id and target_sequence_id must not be empty, "
                "and relation sources must not be empty");
        }
        if (!seen_relation_ids.emplace(item.relation_id).second) {
            return internal::invalidArgument(
                "duplicate relation_id in incremental update: " +
                item.relation_id);
        }
        if (!std::isfinite(item.confidence)) {
            return internal::invalidArgument("relation confidence is invalid");
        }

        std::unordered_map<std::string, bool> source_sequences;
        for (const auto& source : item.sources) {
            if (isBlank(source.source_sequence_id) ||
                !std::isfinite(source.weight)) {
                return internal::invalidArgument(
                    "relation source sequence and weight must be valid");
            }
            if (!source_sequences.emplace(
                    source.source_sequence_id, true).second) {
                return internal::invalidArgument(
                    "duplicate relation source sequence: " +
                    source.source_sequence_id);
            }
            if (const auto* range = std::get_if<RelationLagRange>(
                    &source.lag);
                range != nullptr && range->min > range->max) {
                return internal::invalidArgument(
                    "relation source lag range is invalid");
            }
        }
        relations[item.relation_id] = item;
    }

    const auto count = snapshot.items.size();
    relations_.swap(relations);
    return internal::ok(count, "relation configuration incrementally synchronized");
}

std::optional<RuntimeInstanceConfig> RuntimeConfigRegistry::findInstance(
    const SequenceId& sequence_id) const {
    // Copy the result while holding a shared lock; callers do not retain
    // references into the registry after this method returns.
    std::shared_lock lock(mutex_);
    const auto found = instance_configs_.find(sequence_id);
    if (found == instance_configs_.end()) {
        return std::nullopt;
    }
    return found->second;
}

std::optional<SequenceId> RuntimeConfigRegistry::resolveSequenceId(
    const std::string& data_source_id,
    const std::string& external_sequence_id) const {
    // External-id resolution is a read-only lookup and can share the lock
    // with other readers; snapshot replacement remains exclusive.
    std::shared_lock lock(mutex_);
    const auto found = external_sequence_index_.find(
        externalKey(data_source_id, external_sequence_id));
    if (found == external_sequence_index_.end()) {
        return std::nullopt;
    }
    return found->second;
}

std::optional<RuntimeRelationConfig> RuntimeConfigRegistry::findRelation(
    const std::string& relation_id) const {
    std::shared_lock lock(mutex_);
    const auto found = relations_.find(relation_id);
    if (found == relations_.end()) {
        return std::nullopt;
    }
    return found->second;
}

ConstraintLookupResult RuntimeConfigRegistry::lookupConstraints(
    const std::vector<std::string>& constraint_ids) const {
    // Return copied rules so the shared lock need not escape this method.
    std::shared_lock lock(mutex_);
    ConstraintLookupResult result;
    result.enabled_rules.reserve(constraint_ids.size());
    for (const auto& constraint_id : constraint_ids) {
        const auto constraint = constraints_.find(constraint_id);
        if (constraint == constraints_.end()) {
            result.missing_ids.push_back(constraint_id);
        } else if (!constraint->second.enabled) {
            result.disabled_ids.push_back(constraint_id);
        } else {
            result.enabled_rules.push_back(constraint->second.rule);
        }
    }
    return result;
}

std::vector<ConstraintRule> RuntimeConfigRegistry::enabledConstraints(
    const std::vector<std::string>& constraint_ids) const {
    return lookupConstraints(constraint_ids).enabled_rules;
}

std::vector<ConstraintRule> RuntimeConfigRegistry::allEnabledConstraints() const {
    // Copy the rules while holding the shared lock, then sort the snapshot so
    // continuous checks and their logs are deterministic despite the
    // unordered runtime index.
    std::shared_lock lock(mutex_);
    std::vector<ConstraintRule> result;
    result.reserve(constraints_.size());
    for (const auto& [constraint_id, config] : constraints_) {
        (void)constraint_id;
        if (config.enabled) {
            result.push_back(config.rule);
        }
    }
    std::sort(
        result.begin(), result.end(),
        [](const ConstraintRule& left, const ConstraintRule& right) {
            return left.constraint_id < right.constraint_id;
        });
    return result;
}

}  // namespace sfkg::timeseries::core
