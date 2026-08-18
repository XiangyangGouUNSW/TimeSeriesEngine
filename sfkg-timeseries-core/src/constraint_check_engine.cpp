#include "sfkg/timeseries/core/constraint_check_engine.hpp"

#include <algorithm>
#include <cmath>
#include <string>
#include <unordered_set>
#include <utility>

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

bool validateRule(
    const ConstraintRule& rule,
    std::size_t* max_offset,
    std::string* error) {
    if (rule.constraint_id.empty()) {
        *error = "constraint_id must not be empty";
        return false;
    }
    if (!std::isfinite(rule.lower_bound) ||
        !std::isfinite(rule.upper_bound) ||
        rule.lower_bound > rule.upper_bound) {
        *error = "constraint bounds must be finite and ordered";
        return false;
    }
    if (rule.variable_mapping.empty() || rule.terms.empty()) {
        *error = "constraint mappings and terms must not be empty";
        return false;
    }

    std::size_t previous_offset = 0;
    bool first_term = true;
    for (const auto& term : rule.terms) {
        if (term.variable.empty() || !std::isfinite(term.coefficient)) {
            *error =
                "constraint terms must contain a variable and finite coefficient";
            return false;
        }
        if (rule.variable_mapping.find(term.variable) ==
            rule.variable_mapping.end()) {
            *error = "constraint term has no variable mapping: " +
                term.variable;
            return false;
        }
        if ((first_term && term.sample_offset != 0) ||
            (!first_term && term.sample_offset < previous_offset)) {
            *error =
                "constraint offsets must start at zero and be nondecreasing";
            return false;
        }
        first_term = false;
        previous_offset = term.sample_offset;
    }
    *max_offset = previous_offset;
    return true;
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
    return value >= rule.lower_bound && value <= rule.upper_bound;
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
                resolved_term.value});
        }
        result->violations.push_back({
            rule.constraint_id,
            anchor_time,
            rule.lower_bound,
            rule.upper_bound,
            evaluated_value,
            std::move(term_values)});
    }
    return true;
}

void finalizeResult(ConstraintCheckResult* result) {
    result->satisfied = result->violations.empty();
    std::string message = result->satisfied
        ? "constraint checks completed; all satisfied"
        : "constraint checks completed; violations found";
    if (result->pending_count != 0) {
        message += "; " + std::to_string(result->pending_count) +
            " aligned samples pending because mapped sequence data was "
            "not available yet";
    }
    result->operation = internal::ok(
        result->evaluated_count,
        std::move(message));
}

bool isMissingMappedSequenceError(const std::string& error) {
    constexpr const char* kPrefix =
        "aligned sample is missing mapped sequence: ";
    return error.compare(0, std::char_traits<char>::length(kPrefix), kPrefix) ==
        0;
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

    ConstraintCheckResult result;
    result.project_id = project_id;
    for (const auto& rule : rules) {
        std::size_t max_offset = 0;
        std::string error;
        if (!validateRule(rule, &max_offset, &error)) {
            return failure(internal::invalidArgument(error));
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
        if (max_offset >= points.size()) {
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
                &result,
                &error,
                &resolved_terms);
            if (!evaluated) {
                return failure(internal::invalidArgument(error));
            }
        }
    }

    finalizeResult(&result);
    return result;
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

    ConstraintCheckResult result;
    result.project_id = project_id;
    for (const auto& rule : rules) {
        std::size_t max_offset = 0;
        std::string error;
        if (!validateRule(rule, &max_offset, &error)) {
            return failure(internal::invalidArgument(error));
        }
        const auto mapped_sequences = mappedSequencesForRule(rule);
        if (max_offset >= data.samples.size()) {
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
                &result,
                &error,
                &resolved_terms);
            if (!evaluated) {
                // A continuous ingest request can arrive before the other
                // RPC lanes carrying the remaining sequence values. This is
                // a temporarily non-evaluable sample, not an invalid rule or
                // malformed aligned window. A later update of the missing
                // sequence will retry the affected range.
                if (isMissingMappedSequenceError(error)) {
                    ++result.pending_count;
                    continue;
                }
                return failure(internal::invalidArgument(error));
            }
        }
    }

    finalizeResult(&result);
    return result;
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

}  // namespace sfkg::timeseries::core
