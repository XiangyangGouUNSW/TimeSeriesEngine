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

bool continuousNumeric(const RuntimeInstanceConfig& config) {
    if (config.series_kind != SeriesKind::Continuous) {
        return false;
    }
    return config.data_type == "double" || config.data_type == "float" ||
        config.data_type == "continuous" || config.data_type == "int" ||
        config.data_type == "int64" || config.data_type == "integer";
}

bool validDerivedOperator(DerivedOperator operation) {
    return operation == DerivedOperator::Add ||
        operation == DerivedOperator::Subtract ||
        operation == DerivedOperator::Multiply ||
        operation == DerivedOperator::Divide;
}

bool containsSequenceLeaf(const DerivedExpression& expression) {
    switch (expression.kind) {
        case DerivedExpression::NodeKind::Sequence:
            return true;
        case DerivedExpression::NodeKind::Constant:
            return false;
        case DerivedExpression::NodeKind::Binary:
            return (expression.binary.left &&
                    containsSequenceLeaf(*expression.binary.left)) ||
                (expression.binary.right &&
                 containsSequenceLeaf(*expression.binary.right));
    }
    return false;
}

bool validateDerivedExpression(
    const DerivedExpression& expression,
    const std::unordered_map<SequenceId, RuntimeInstanceConfig>& instances,
    std::string* error) {
    switch (expression.kind) {
        case DerivedExpression::NodeKind::Sequence: {
            if (expression.sequence_id.empty()) {
                *error = "derived expression sequence_id must not be empty";
                return false;
            }
            const auto found = instances.find(expression.sequence_id);
            if (found == instances.end() || !continuousNumeric(found->second)) {
                *error = "derived expression sequence must be registered as a "
                    "continuous numeric sequence: " + expression.sequence_id;
                return false;
            }
            return true;
        }
        case DerivedExpression::NodeKind::Constant:
            if (!std::isfinite(expression.constant)) {
                *error = "derived expression constant must be finite";
                return false;
            }
            return true;
        case DerivedExpression::NodeKind::Binary:
            if (!validDerivedOperator(expression.binary.operation) ||
                !expression.binary.left || !expression.binary.right) {
                *error = "derived binary expression is incomplete or invalid";
                return false;
            }
            return validateDerivedExpression(
                       *expression.binary.left, instances, error) &&
                validateDerivedExpression(
                    *expression.binary.right, instances, error);
    }
    *error = "unknown derived expression node";
    return false;
}

bool validateDerivedConfig(
    const RuntimeDerivedSeriesConfig& item,
    const std::unordered_map<SequenceId, RuntimeInstanceConfig>& instances,
    std::string* error) {
    if (item.derived_sequence_id.empty()) {
        *error = "derived_sequence_id must not be empty";
        return false;
    }
    if (instances.find(item.derived_sequence_id) != instances.end()) {
        *error = "derived sequence_id conflicts with an instance sequence: " +
            item.derived_sequence_id;
        return false;
    }

    if (const auto* linear = std::get_if<DerivedLinearCombination>(
            &item.formula);
        linear != nullptr) {
        if (linear->terms.empty() || !std::isfinite(linear->bias)) {
            *error = "derived linear combination must contain finite terms and bias";
            return false;
        }
        std::unordered_set<SequenceId> seen;
        for (const auto& term : linear->terms) {
            if (term.sequence_id.empty() || !std::isfinite(term.coefficient)) {
                *error = "derived linear term is invalid";
                return false;
            }
            const auto found = instances.find(term.sequence_id);
            if (found == instances.end() || !continuousNumeric(found->second)) {
                *error = "derived linear term must reference a registered "
                    "continuous numeric sequence: " + term.sequence_id;
                return false;
            }
            if (!seen.emplace(term.sequence_id).second) {
                *error = "duplicate derived linear term sequence: " +
                    term.sequence_id;
                return false;
            }
        }
        return true;
    }

    if (const auto* expression = std::get_if<DerivedExpression>(
            &item.formula);
        expression != nullptr) {
        if (!containsSequenceLeaf(*expression)) {
            *error = "derived expression must reference at least one sequence";
            return false;
        }
        return validateDerivedExpression(*expression, instances, error);
    }

    *error = "derived formula is not set";
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

OperationResult RuntimeConfigRegistry::upsertDerivedSeriesConfigs(
    const RuntimeConfigSnapshot<RuntimeDerivedSeriesConfig>& snapshot) {
    std::unique_lock lock(mutex_);
    auto derived_series = derived_series_;
    std::unordered_set<SequenceId> seen_ids;
    for (const auto& item : snapshot.items) {
        if (!seen_ids.emplace(item.derived_sequence_id).second) {
            return internal::invalidArgument(
                "duplicate derived_sequence_id in incremental update: " +
                item.derived_sequence_id);
        }
        std::string error;
        if (!validateDerivedConfig(item, instance_configs_, &error)) {
            return internal::invalidArgument(error);
        }
        derived_series[item.derived_sequence_id] = item;
    }
    const auto count = snapshot.items.size();
    derived_series_.swap(derived_series);
    return internal::ok(
        count, "derived series configuration incrementally synchronized");
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

std::vector<RuntimeDerivedSeriesConfig>
RuntimeConfigRegistry::allDerivedSeries() const {
    std::shared_lock lock(mutex_);
    std::vector<RuntimeDerivedSeriesConfig> result;
    result.reserve(derived_series_.size());
    for (const auto& [sequence_id, config] : derived_series_) {
        (void)sequence_id;
        result.push_back(config);
    }
    std::sort(
        result.begin(), result.end(),
        [](const auto& left, const auto& right) {
            return left.derived_sequence_id < right.derived_sequence_id;
        });
    return result;
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
