#pragma once

#include <cmath>
#include <cstddef>
#include <string>

#include "sfkg/timeseries/core/types.hpp"

namespace sfkg::timeseries::core::internal {

enum class ConstraintRuleKind {
    Sample,
    WindowAggregate
};

struct ConstraintRuleProperties {
    ConstraintRuleKind kind{ConstraintRuleKind::Sample};
    std::size_t maximum_sample_offset{0};
};

inline bool isWindowAggregation(ConstraintAggregation aggregation) {
    switch (aggregation) {
        case ConstraintAggregation::Sample:
            return false;
        case ConstraintAggregation::Average:
        case ConstraintAggregation::Maximum:
        case ConstraintAggregation::Minimum:
            return true;
    }
    return false;
}

inline bool validConstraintAggregation(ConstraintAggregation aggregation) {
    switch (aggregation) {
        case ConstraintAggregation::Sample:
        case ConstraintAggregation::Average:
        case ConstraintAggregation::Maximum:
        case ConstraintAggregation::Minimum:
            return true;
    }
    return false;
}

inline bool validateConstraintRule(
    const ConstraintRule& rule,
    ConstraintRuleProperties* properties,
    std::string* error) {
    if (rule.constraint_id.empty()) {
        *error = "constraint_id must not be empty";
        return false;
    }
    if (!rule.lower_bound && !rule.upper_bound) {
        *error = "constraint must contain a lower or upper bound";
        return false;
    }
    if ((rule.lower_bound && !std::isfinite(*rule.lower_bound)) ||
        (rule.upper_bound && !std::isfinite(*rule.upper_bound)) ||
        (rule.lower_bound && rule.upper_bound &&
         *rule.lower_bound > *rule.upper_bound)) {
        *error = "constraint bounds must be finite and ordered";
        return false;
    }
    if (rule.variable_mapping.empty() || rule.terms.empty()) {
        *error = "constraint mappings and terms must not be empty";
        return false;
    }

    bool has_sample_term = false;
    bool has_aggregate_term = false;
    std::size_t previous_offset = 0;
    bool first_sample_term = true;
    for (const auto& term : rule.terms) {
        if (term.variable.empty() || !std::isfinite(term.coefficient)) {
            *error =
                "constraint terms must contain a variable and finite coefficient";
            return false;
        }
        if (!validConstraintAggregation(term.aggregation)) {
            *error = "constraint term has an unknown aggregation";
            return false;
        }
        if (rule.variable_mapping.find(term.variable) ==
            rule.variable_mapping.end()) {
            *error = "constraint term has no variable mapping: " +
                term.variable;
            return false;
        }

        if (isWindowAggregation(term.aggregation)) {
            has_aggregate_term = true;
            if (term.sample_offset != 0) {
                *error =
                    "window aggregate constraint terms must use sample_offset zero";
                return false;
            }
            continue;
        }

        has_sample_term = true;
        if ((first_sample_term && term.sample_offset != 0) ||
            (!first_sample_term && term.sample_offset < previous_offset)) {
            *error =
                "constraint offsets must start at zero and be nondecreasing";
            return false;
        }
        first_sample_term = false;
        previous_offset = term.sample_offset;
    }

    if (has_sample_term && has_aggregate_term) {
        *error =
            "sample and window aggregate terms must not be mixed in one rule";
        return false;
    }

    if (properties != nullptr) {
        properties->kind = has_aggregate_term
            ? ConstraintRuleKind::WindowAggregate
            : ConstraintRuleKind::Sample;
        properties->maximum_sample_offset = has_sample_term
            ? previous_offset
            : 0;
    }
    return true;
}

}  // namespace sfkg::timeseries::core::internal
