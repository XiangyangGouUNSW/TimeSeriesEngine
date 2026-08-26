#include "sfkg/timeseries/core/constraint_check_engine.hpp"

#include <algorithm>
#include <cmath>
#include <iterator>
#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "constraint_rule_helpers.hpp"
#include "operation_helpers.hpp"

namespace sfkg::timeseries::core {
namespace {

OperationResult failedPrecondition(std::string message) {
    return internal::makeOperationResult(
        OperationCode::FailedPrecondition, 0, 0, std::move(message));
}

ConstraintCheckResult failure(OperationResult operation) {
    ConstraintCheckResult result;
    result.operation = std::move(operation);
    result.satisfied = false;
    return result;
}

bool numericValue(const TimeseriesValue& value, double* output) {
    if (const auto* number = std::get_if<double>(&value)) {
        *output = *number;
        return std::isfinite(*output);
    }
    if (const auto* number = std::get_if<std::int64_t>(&value)) {
        *output = static_cast<double>(*number);
        return true;
    }
    return false;
}

bool hasSingleSequence(const ConstraintRule& rule, SequenceId* sequence_id) {
    std::unordered_set<SequenceId> sequence_ids;
    for (const auto& [variable, mapped_sequence] : rule.variable_mapping) {
        (void)variable;
        sequence_ids.insert(mapped_sequence);
    }
    if (sequence_ids.size() != 1) {
        return false;
    }
    *sequence_id = *sequence_ids.begin();
    return true;
}

bool isWithinBounds(double value, const ConstraintRule& rule) {
    return (!rule.lower_bound || value >= *rule.lower_bound) &&
        (!rule.upper_bound || value <= *rule.upper_bound);
}

struct ResolvedTermValue {
    Timestamp sample_time{};
    double value{};
};

std::vector<SequenceId> mappedSequencesForRule(
    const ConstraintRule& rule) {
    std::vector<SequenceId> mapped_sequences;
    mapped_sequences.reserve(rule.terms.size());
    for (const auto& term : rule.terms) {
        mapped_sequences.push_back(rule.variable_mapping.at(term.variable));
    }
    return mapped_sequences;
}

template <typename ResolveTerm>
bool evaluateRuleAt(
    const ConstraintRule& rule,
    const std::vector<SequenceId>& mapped_sequences,
    Timestamp anchor_time,
    std::size_t anchor,
    ResolveTerm&& resolve_term,
    ConstraintCheckResult* result,
    std::string* error,
    std::vector<ResolvedTermValue>* resolved_terms) {
    double evaluated_value = 0.0;
    resolved_terms->clear();
    resolved_terms->resize(rule.terms.size());

    for (std::size_t term_index = 0;
         term_index < rule.terms.size();
         ++term_index) {
        const auto& term = rule.terms[term_index];
        const auto& mapped_sequence = mapped_sequences[term_index];
        auto& term_value = (*resolved_terms)[term_index];
        if (!resolve_term(
                term, mapped_sequence, anchor, &term_value, error)) {
            return false;
        }
        evaluated_value += term.coefficient * term_value.value;
    }

    ++result->evaluated_count;
    if (!isWithinBounds(evaluated_value, rule)) {
        std::vector<ConstraintTermValue> term_values;
        term_values.reserve(rule.terms.size());
        for (std::size_t term_index = 0;
             term_index < rule.terms.size();
             ++term_index) {
            const auto& term = rule.terms[term_index];
            const auto& resolved_term = (*resolved_terms)[term_index];
            term_values.push_back({
                term.variable,
                mapped_sequences[term_index],
                term.coefficient,
                term.sample_offset,
                resolved_term.sample_time,
                resolved_term.value,
                term.aggregation});
        }
        result->violations.push_back({
            rule.constraint_id,
            anchor_time,
            rule.lower_bound,
            rule.upper_bound,
            evaluated_value,
            std::move(term_values),
            rule.or_group_id});
    }
    return true;
}

struct RuleCheckOutcome {
    const ConstraintRule* rule{};
    ConstraintCheckResult result;
};

ConstraintCheckResult combineRuleOutcomes(
    const ProjectId& project_id,
    std::vector<RuleCheckOutcome> outcomes) {
    ConstraintCheckResult result;
    result.project_id = project_id;

    struct Clause {
        std::vector<std::size_t> outcome_indices;
    };
    std::vector<Clause> clauses;
    std::unordered_map<std::string, std::size_t> clause_index;
    clause_index.reserve(outcomes.size());

    for (std::size_t index = 0; index < outcomes.size(); ++index) {
        auto& outcome = outcomes[index];
        result.evaluated_count += outcome.result.evaluated_count;
        result.pending_count += outcome.result.pending_count;
        const auto& rule = *outcome.rule;
        const std::string key = rule.or_group_id.empty()
            ? std::string{"\x1f"} + rule.constraint_id
            : std::string{"\x1e"} + rule.or_group_id;
        auto [found, inserted] = clause_index.emplace(key, clauses.size());
        if (inserted) {
            clauses.emplace_back();
        }
        clauses[found->second].outcome_indices.push_back(index);
    }

    result.satisfied = true;
    for (const auto& clause : clauses) {
        const bool clause_satisfied = std::any_of(
            clause.outcome_indices.begin(),
            clause.outcome_indices.end(),
            [&outcomes](std::size_t index) {
                return outcomes[index].result.violations.empty();
            });
        if (clause_satisfied) {
            continue;
        }
        result.satisfied = false;
        for (const auto index : clause.outcome_indices) {
            auto& violations = outcomes[index].result.violations;
            result.violations.insert(
                result.violations.end(),
                std::make_move_iterator(violations.begin()),
                std::make_move_iterator(violations.end()));
        }
    }

    std::string message = result.satisfied
        ? "constraint checks completed; all satisfied"
        : "constraint checks completed; violations found";
    if (result.pending_count != 0) {
        message += "; " + std::to_string(result.pending_count) +
            " aligned samples pending because mapped sequence data was "
            "not available yet";
    }
    result.operation = internal::ok(result.evaluated_count, std::move(message));
    return result;
}

bool isMissingMappedSequenceError(const std::string& error) {
    constexpr const char* kPrefix =
        "aligned sample is missing mapped sequence: ";
    return error.compare(0, std::char_traits<char>::length(kPrefix), kPrefix) ==
        0;
}

bool buildWindowStatisticsForRule(
    const ConstraintRule& rule,
    const WindowData& data,
    WindowStatisticsData* statistics,
    std::string* error) {
    statistics->window_start_time = data.window_start_time;
    statistics->window_end_time = data.window_end_time;
    statistics->project_id = data.project_id;
    statistics->sequence_statistics.clear();

    std::unordered_set<SequenceId> seen;
    for (const auto& term : rule.terms) {
        const auto& sequence_id = rule.variable_mapping.at(term.variable);
        if (!seen.insert(sequence_id).second) {
            continue;
        }
        const auto sequence = data.sequence_values.find(sequence_id);
        if (sequence == data.sequence_values.end()) {
            *error = "window data does not contain mapped sequence: " +
                sequence_id;
            return false;
        }

        std::size_t count = 0;
        long double sum = 0.0L;
        double minimum = std::numeric_limits<double>::infinity();
        double maximum = -std::numeric_limits<double>::infinity();
        for (const auto& point : sequence->second) {
            if (point.time < data.window_start_time ||
                point.time >= data.window_end_time) {
                continue;
            }
            double value = 0.0;
            if (!numericValue(point.value, &value)) {
                *error =
                    "window aggregate constraint values must be finite numeric values";
                return false;
            }
            ++count;
            sum += static_cast<long double>(value);
            minimum = std::min(minimum, value);
            maximum = std::max(maximum, value);
        }
        if (count == 0) {
            continue;
        }
        const auto average = static_cast<double>(
            sum / static_cast<long double>(count));
        if (!std::isfinite(average)) {
            *error = "window aggregate average is not finite";
            return false;
        }
        statistics->sequence_statistics.emplace(
            sequence_id,
            SequenceWindowStatistics{count, average, maximum, minimum});
    }
    return true;
}

bool evaluateAggregateRule(
    const ConstraintRule& rule,
    const WindowStatisticsData& data,
    ConstraintCheckResult* result,
    std::string* error) {
    const auto mapped_sequences = mappedSequencesForRule(rule);
    for (const auto& sequence_id : mapped_sequences) {
        if (data.sequence_statistics.find(sequence_id) ==
            data.sequence_statistics.end()) {
            ++result->pending_count;
            return true;
        }
    }

    std::vector<ResolvedTermValue> resolved_terms;
    resolved_terms.reserve(rule.terms.size());
    return evaluateRuleAt(
        rule,
        mapped_sequences,
        data.window_end_time,
        0,
        [&data](const ConstraintTerm& term,
                const SequenceId& sequence_id,
                std::size_t,
                ResolvedTermValue* term_value,
                std::string* resolve_error) {
            const auto found = data.sequence_statistics.find(sequence_id);
            if (found == data.sequence_statistics.end() ||
                found->second.count == 0) {
                *resolve_error =
                    "window aggregate sequence has no numeric samples: " +
                    sequence_id;
                return false;
            }
            switch (term.aggregation) {
                case ConstraintAggregation::Average:
                    term_value->value = found->second.average;
                    break;
                case ConstraintAggregation::Maximum:
                    term_value->value = found->second.maximum;
                    break;
                case ConstraintAggregation::Minimum:
                    term_value->value = found->second.minimum;
                    break;
                case ConstraintAggregation::Sample:
                    *resolve_error =
                        "sample term cannot be evaluated from window statistics";
                    return false;
            }
            term_value->sample_time = data.window_end_time;
            return true;
        },
        result,
        error,
        &resolved_terms);
}

}  // namespace

ConstraintCheckResult ConstraintCheckEngine::checkConstraints(
    const ProjectId& project_id,
    const std::vector<ConstraintRule>& rules,
    const WindowData& data) const {
    return checkConstraints(project_id, rules, data, std::nullopt);
}

ConstraintCheckResult ConstraintCheckEngine::checkConstraints(
    const std::vector<ConstraintRule>& rules,
    const WindowData& data) const {
    return checkConstraints(
        data.project_id.empty() ? ProjectId{"default"} : data.project_id,
        rules,
        data);
}

ConstraintCheckResult ConstraintCheckEngine::checkConstraints(
    const ProjectId& project_id,
    const std::vector<ConstraintRule>& rules,
    const WindowData& data,
    const std::optional<ConstraintCheckRange>& range) const {
    (void)project_id;
    if (rules.empty()) {
        return failure(internal::invalidArgument(
            "constraint rules must not be empty"));
    }
    if (data.window_start_time > data.window_end_time) {
        return failure(internal::invalidArgument(
            "window start time must not be after window end time"));
    }
    if (data.sequence_values.empty()) {
        return failure(internal::invalidArgument(
            "window data must contain at least one sequence"));
    }

    std::vector<RuleCheckOutcome> outcomes;
    outcomes.reserve(rules.size());
    for (const auto& rule : rules) {
        internal::ConstraintRuleProperties properties;
        std::string error;
        if (!internal::validateConstraintRule(
                rule, &properties, &error)) {
            return failure(internal::invalidArgument(error));
        }

        ConstraintCheckResult rule_result;
        rule_result.project_id = project_id;
        if (properties.kind ==
            internal::ConstraintRuleKind::WindowAggregate) {
            WindowStatisticsData statistics;
            if (!buildWindowStatisticsForRule(
                    rule, data, &statistics, &error) ||
                !evaluateAggregateRule(
                    rule, statistics, &rule_result, &error)) {
                return failure(internal::invalidArgument(error));
            }
            outcomes.push_back({&rule, std::move(rule_result)});
            continue;
        }

        SequenceId sequence_id;
        if (!hasSingleSequence(rule, &sequence_id)) {
            return failure(failedPrecondition(
                "WindowData can only check a rule mapped to one sequence; "
                "use AlignedWindowData for multi-sequence rules"));
        }
        const auto mapped_sequences = mappedSequencesForRule(rule);
        const auto sequence = data.sequence_values.find(sequence_id);
        if (sequence == data.sequence_values.end()) {
            return failure(internal::invalidArgument(
                "window data does not contain mapped sequence: " +
                sequence_id));
        }

        const auto& points = sequence->second;
        const auto max_offset = properties.maximum_sample_offset;
        if (max_offset >= points.size()) {
            outcomes.push_back({&rule, std::move(rule_result)});
            continue;
        }

        std::vector<ResolvedTermValue> resolved_terms;
        resolved_terms.reserve(rule.terms.size());
        std::size_t first_anchor = 0;
        if (range) {
            first_anchor = static_cast<std::size_t>(std::lower_bound(
                points.begin(), points.end(), range->start_time,
                [](const RawTimeseriesPoint& point, Timestamp time) {
                    return point.time < time;
                }) - points.begin());
        }
        for (std::size_t anchor = first_anchor;
             anchor + max_offset < points.size();
             ++anchor) {
            if (range && points[anchor].time > range->end_time) {
                break;
            }
            const bool evaluated = evaluateRuleAt(
                rule,
                mapped_sequences,
                points[anchor].time,
                anchor,
                [&](const ConstraintTerm& term,
                    const SequenceId& mapped_sequence,
                    std::size_t rule_anchor,
                    ResolvedTermValue* term_value,
                    std::string* resolve_error) {
                    const auto& point = points[rule_anchor + term.sample_offset];
                    if (mapped_sequence != point.sequence_id) {
                        *resolve_error =
                            "window point sequence_id does not match its map "
                            "key";
                        return false;
                    }
                    double value = 0.0;
                    if (!numericValue(point.value, &value)) {
                        *resolve_error =
                            "constraint value must be a finite numeric value";
                        return false;
                    }
                    term_value->sample_time = point.time;
                    term_value->value = value;
                    return true;
                },
                &rule_result,
                &error,
                &resolved_terms);
            if (!evaluated) {
                return failure(internal::invalidArgument(error));
            }
        }
        outcomes.push_back({&rule, std::move(rule_result)});
    }

    return combineRuleOutcomes(project_id, std::move(outcomes));
}

ConstraintCheckResult ConstraintCheckEngine::checkConstraints(
    const std::vector<ConstraintRule>& rules,
    const WindowData& data,
    const std::optional<ConstraintCheckRange>& range) const {
    return checkConstraints(
        data.project_id.empty() ? ProjectId{"default"} : data.project_id,
        rules,
        data,
        range);
}

ConstraintCheckResult ConstraintCheckEngine::checkConstraints(
    const ProjectId& project_id,
    const std::vector<ConstraintRule>& rules,
    const AlignedWindowData& data) const {
    return checkConstraints(project_id, rules, data, std::nullopt);
}

ConstraintCheckResult ConstraintCheckEngine::checkConstraints(
    const std::vector<ConstraintRule>& rules,
    const AlignedWindowData& data) const {
    return checkConstraints(
        data.project_id.empty() ? ProjectId{"default"} : data.project_id,
        rules,
        data);
}

ConstraintCheckResult ConstraintCheckEngine::checkConstraints(
    const ProjectId& project_id,
    const std::vector<ConstraintRule>& rules,
    const AlignedWindowData& data,
    const std::optional<ConstraintCheckRange>& range) const {
    (void)project_id;
    if (rules.empty()) {
        return failure(internal::invalidArgument(
            "constraint rules must not be empty"));
    }
    if (data.window_start_time > data.window_end_time) {
        return failure(internal::invalidArgument(
            "aligned window start time must not be after window end time"));
    }
    if (data.samples.empty()) {
        return failure(internal::invalidArgument(
            "aligned window must contain at least one sample"));
    }
    for (std::size_t index = 1; index < data.samples.size(); ++index) {
        if (data.samples[index - 1].time >= data.samples[index].time) {
            return failure(internal::invalidArgument(
                "aligned sample times must be strictly increasing"));
        }
    }

    std::vector<RuleCheckOutcome> outcomes;
    outcomes.reserve(rules.size());
    for (const auto& rule : rules) {
        internal::ConstraintRuleProperties properties;
        std::string error;
        if (!internal::validateConstraintRule(
                rule, &properties, &error)) {
            return failure(internal::invalidArgument(error));
        }
        if (properties.kind ==
            internal::ConstraintRuleKind::WindowAggregate) {
            return failure(failedPrecondition(
                "window aggregate rules require raw window statistics; "
                "they cannot be evaluated from aligned samples"));
        }
        ConstraintCheckResult rule_result;
        rule_result.project_id = project_id;
        const auto mapped_sequences = mappedSequencesForRule(rule);
        const auto max_offset = properties.maximum_sample_offset;
        if (max_offset >= data.samples.size()) {
            outcomes.push_back({&rule, std::move(rule_result)});
            continue;
        }

        std::vector<ResolvedTermValue> resolved_terms;
        resolved_terms.reserve(rule.terms.size());
        error.clear();
        std::size_t first_anchor = 0;
        if (range) {
            first_anchor = static_cast<std::size_t>(std::lower_bound(
                data.samples.begin(), data.samples.end(), range->start_time,
                [](const AlignedSample& sample, Timestamp time) {
                    return sample.time < time;
                }) - data.samples.begin());
        }
        for (std::size_t anchor = first_anchor;
             anchor + max_offset < data.samples.size();
             ++anchor) {
            if (range && data.samples[anchor].time > range->end_time) {
                break;
            }
            const bool evaluated = evaluateRuleAt(
                rule,
                mapped_sequences,
                data.samples[anchor].time,
                anchor,
                [&](const ConstraintTerm& term,
                    const SequenceId& mapped_sequence,
                    std::size_t rule_anchor,
                    ResolvedTermValue* term_value,
                    std::string* resolve_error) {
                    const auto& sample =
                        data.samples[rule_anchor + term.sample_offset];
                    const auto value_it = sample.values.find(mapped_sequence);
                    if (value_it == sample.values.end()) {
                        *resolve_error =
                            "aligned sample is missing mapped sequence: " +
                            mapped_sequence;
                        return false;
                    }
                    double value = 0.0;
                    if (!numericValue(value_it->second, &value)) {
                        *resolve_error =
                            "constraint value must be a finite numeric value";
                        return false;
                    }
                    term_value->sample_time = sample.time;
                    term_value->value = value;
                    return true;
                },
                &rule_result,
                &error,
                &resolved_terms);
            if (!evaluated) {
                // A continuous ingest request can arrive before the other
                // RPC lanes carrying the remaining sequence values. This is
                // a temporarily non-evaluable sample, not an invalid rule or
                // malformed aligned window. A later update of the missing
                // sequence will retry the affected range.
                if (isMissingMappedSequenceError(error)) {
                    ++rule_result.pending_count;
                    continue;
                }
                return failure(internal::invalidArgument(error));
            }
        }
        outcomes.push_back({&rule, std::move(rule_result)});
    }

    return combineRuleOutcomes(project_id, std::move(outcomes));
}

ConstraintCheckResult ConstraintCheckEngine::checkConstraints(
    const std::vector<ConstraintRule>& rules,
    const AlignedWindowData& data,
    const std::optional<ConstraintCheckRange>& range) const {
    return checkConstraints(
        data.project_id.empty() ? ProjectId{"default"} : data.project_id,
        rules,
        data,
        range);
}

ConstraintCheckResult ConstraintCheckEngine::checkConstraints(
    const ProjectId& project_id,
    const std::vector<ConstraintRule>& rules,
    const WindowStatisticsData& data) const {
    if (rules.empty()) {
        return failure(internal::invalidArgument(
            "constraint rules must not be empty"));
    }
    if (data.window_start_time > data.window_end_time) {
        return failure(internal::invalidArgument(
            "window statistics start time must not be after end time"));
    }

    std::vector<RuleCheckOutcome> outcomes;
    outcomes.reserve(rules.size());
    for (const auto& rule : rules) {
        internal::ConstraintRuleProperties properties;
        std::string error;
        if (!internal::validateConstraintRule(
                rule, &properties, &error)) {
            return failure(internal::invalidArgument(error));
        }
        if (properties.kind !=
            internal::ConstraintRuleKind::WindowAggregate) {
            return failure(failedPrecondition(
                "sample rules cannot be evaluated from window statistics"));
        }

        ConstraintCheckResult rule_result;
        rule_result.project_id = project_id;
        if (!evaluateAggregateRule(rule, data, &rule_result, &error)) {
            return failure(internal::invalidArgument(error));
        }
        outcomes.push_back({&rule, std::move(rule_result)});
    }
    return combineRuleOutcomes(project_id, std::move(outcomes));
}

ConstraintCheckResult ConstraintCheckEngine::checkConstraints(
    const std::vector<ConstraintRule>& rules,
    const WindowStatisticsData& data) const {
    return checkConstraints(
        data.project_id.empty() ? ProjectId{"default"} : data.project_id,
        rules,
        data);
}

}  // namespace sfkg::timeseries::core
