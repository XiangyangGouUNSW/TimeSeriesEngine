#include "sfkg/timeseries/core/runtime_config_registry.hpp"

#include <algorithm>
#include <cmath>
#include <mutex>
#include <shared_mutex>
#include <string>
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
        std::unique_lock lock(mutex_);
        instance_configs_.swap(instances);
        external_sequence_index_.swap(external_index);
    }
    return internal::ok(count, "instance configuration snapshot replaced");
}

OperationResult RuntimeConfigRegistry::replaceConstraints(
    const RuntimeConfigSnapshot<RuntimeConstraintConfig>& snapshot) {
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

OperationResult RuntimeConfigRegistry::replaceRelations(
    const RuntimeConfigSnapshot<RuntimeRelationConfig>& snapshot) {
    std::unordered_map<std::string, RuntimeRelationConfig> relations;
    for (const auto& item : snapshot.items) {
        if (isBlank(item.relation_id) || isBlank(item.target_category_id) ||
            item.sources.empty()) {
            return internal::invalidArgument(
                "relation_id and target_category_id must not be empty, "
                "and relation sources must not be empty");
        }
        if (!std::isfinite(item.confidence)) {
            return internal::invalidArgument(
                "relation confidence is invalid");
        }
        std::unordered_map<std::string, bool> source_categories;
        for (const auto& source : item.sources) {
            if (isBlank(source.source_category_id) ||
                !std::isfinite(source.weight)) {
                return internal::invalidArgument(
                    "relation source category and weight must be valid");
            }
            if (!source_categories.emplace(
                    source.source_category_id, true).second) {
                return internal::invalidArgument(
                    "duplicate relation source category: " +
                    source.source_category_id);
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
        std::unique_lock lock(mutex_);
        relations_.swap(relations);
    }
    return internal::ok(count, "relation configuration snapshot replaced");
}

std::optional<RuntimeInstanceConfig> RuntimeConfigRegistry::findInstance(
    const SequenceId& sequence_id) const {
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
    std::shared_lock lock(mutex_);
    const auto found = external_sequence_index_.find(
        externalKey(data_source_id, external_sequence_id));
    if (found == external_sequence_index_.end()) {
        return std::nullopt;
    }
    return found->second;
}

std::vector<ConstraintRule> RuntimeConfigRegistry::enabledConstraints(
    const std::vector<std::string>& constraint_ids) const {
    std::shared_lock lock(mutex_);
    std::vector<ConstraintRule> result;
    result.reserve(constraint_ids.size());
    for (const auto& constraint_id : constraint_ids) {
        const auto constraint = constraints_.find(constraint_id);
        if (constraint != constraints_.end() && constraint->second.enabled) {
            result.push_back(constraint->second.rule);
        }
    }
    return result;
}

}  // namespace sfkg::timeseries::core
