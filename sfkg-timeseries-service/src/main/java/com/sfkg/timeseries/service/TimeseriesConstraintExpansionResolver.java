package com.sfkg.timeseries.service;

import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

@Component
public class TimeseriesConstraintExpansionResolver {

    private final TimeseriesMemoryCache memoryCache;

    public TimeseriesConstraintExpansionResolver(TimeseriesMemoryCache memoryCache) {
        this.memoryCache = memoryCache;
    }

    public List<ExpandedConstraintRule> expandConstraint(TimeseriesConstraint constraint) {
        if (constraint == null) {
            return List.of();
        }
        Map<String, String> rawMapping = constraint.getVariableMapping() != null
                ? constraint.getVariableMapping()
                : Map.of();
        return expandConstraintRules(constraint.getConstraintId(), rawMapping);
    }

    public List<String> expandConstraintIdsForContext(
            TimeseriesConstraint constraint,
            Collection<String> targetSequenceIds,
            Collection<String> contextSequenceIds,
            boolean explicit) {

        Set<String> targets = targetSequenceIds != null
                ? targetSequenceIds.stream().filter(id -> id != null && !id.isBlank()).collect(Collectors.toSet())
                : Set.of();
        Set<String> context = contextSequenceIds != null
                ? contextSequenceIds.stream().filter(id -> id != null && !id.isBlank()).collect(Collectors.toSet())
                : Set.of();

        return expandConstraint(constraint).stream()
                .filter(rule -> isRuleApplicable(rule, targets, context, explicit))
                .map(ExpandedConstraintRule::constraintId)
                .distinct()
                .toList();
    }

    private boolean isRuleApplicable(
            ExpandedConstraintRule rule,
            Set<String> targetSequenceIds,
            Set<String> contextSequenceIds,
            boolean explicit) {

        Collection<String> mappedSequenceIds = rule.variableMapping().values().stream()
                .filter(value -> value != null && !value.isBlank())
                .toList();
        if (mappedSequenceIds.isEmpty()) {
            return explicit;
        }
        if (!contextSequenceIds.isEmpty() && !contextSequenceIds.containsAll(mappedSequenceIds)) {
            return false;
        }
        if (explicit) {
            return true;
        }
        return mappedSequenceIds.stream().anyMatch(targetSequenceIds::contains);
    }

    private List<ExpandedConstraintRule> expandConstraintRules(
            String constraintId, Map<String, String> rawMapping) {

        Map<String, List<String>> expandedByVar = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : rawMapping.entrySet()) {
            List<String> seqIds = resolveToSequences(
                    entry.getValue() != null ? List.of(entry.getValue()) : List.of());
            expandedByVar.put(entry.getKey(), seqIds);
        }

        String firstVar = expandedByVar.keySet().stream().findFirst().orElse(null);
        Map<String, Map<String, String>> deviceRules = new LinkedHashMap<>();

        if (firstVar != null && !expandedByVar.get(firstVar).isEmpty()) {
            for (String seqId : expandedByVar.get(firstVar)) {
                TimeseriesInstanceConfig inst = memoryCache.getInstanceBySequenceId(seqId);
                String deviceId = inst != null && inst.getDeviceInstanceId() != null
                        ? inst.getDeviceInstanceId()
                        : "_default";
                String ruleKey = deviceId + "::" + seqId;
                Map<String, String> varMap = new LinkedHashMap<>();
                varMap.put(firstVar, seqId);

                boolean allMatched = true;
                for (Map.Entry<String, List<String>> varEntry : expandedByVar.entrySet()) {
                    if (varEntry.getKey().equals(firstVar)) {
                        continue;
                    }
                    String match = varEntry.getValue().stream()
                            .filter(candidate -> {
                                TimeseriesInstanceConfig candidateInst = memoryCache.getInstanceBySequenceId(candidate);
                                return candidateInst != null && deviceId.equals(candidateInst.getDeviceInstanceId());
                            })
                            .findFirst()
                            .orElse(null);
                    if (match == null) {
                        allMatched = false;
                        break;
                    }
                    varMap.put(varEntry.getKey(), match);
                }

                if (allMatched) {
                    deviceRules.computeIfAbsent(ruleKey, key -> new LinkedHashMap<>()).putAll(varMap);
                }
            }
        } else {
            deviceRules.put("_default", new LinkedHashMap<>(rawMapping));
        }

        List<ExpandedConstraintRule> result = new ArrayList<>();
        for (Map<String, String> varMap : deviceRules.values()) {
            result.add(new ExpandedConstraintRule(
                    buildExpandedConstraintId(constraintId, varMap),
                    new LinkedHashMap<>(varMap)));
        }
        return result;
    }

    private List<String> resolveToSequences(Collection<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        for (String id : ids) {
            if (id == null || id.isBlank()) {
                continue;
            }
            if (memoryCache.getCategory(id).isPresent()) {
                for (TimeseriesInstanceConfig inst : memoryCache.listInstanceConfigs()) {
                    if (id.equals(inst.getCategoryId()) && inst.getSequenceId() != null) {
                        result.add(inst.getSequenceId());
                    }
                }
            } else {
                result.add(id);
            }
        }
        return result;
    }

    private String buildExpandedConstraintId(String constraintId, Map<String, String> varMap) {
        String suffix = varMap.values().stream()
                .filter(value -> value != null && !value.isBlank())
                .distinct()
                .sorted()
                .collect(Collectors.joining("_"));
        return suffix.isEmpty()
                ? nullToEmpty(constraintId)
                : nullToEmpty(constraintId) + "_" + suffix;
    }

    private String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    public record ExpandedConstraintRule(String constraintId, Map<String, String> variableMapping) {
    }
}
