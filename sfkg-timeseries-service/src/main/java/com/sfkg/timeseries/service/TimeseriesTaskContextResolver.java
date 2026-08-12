package com.sfkg.timeseries.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.entity.TimeseriesAnomalyTask;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.entity.TimeseriesForecastTask;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.entity.TimeseriesRelation;
import com.sfkg.timeseries.grpc.SemanticContext;
import com.sfkg.timeseries.grpc.SequenceMetadata;
import com.sfkg.timeseries.grpc.SequenceRelation;

@Component
public class TimeseriesTaskContextResolver {

    private final TimeseriesMemoryCache memoryCache;

    public TimeseriesTaskContextResolver(TimeseriesMemoryCache memoryCache) {
        this.memoryCache = memoryCache;
    }

    public Set<String> resolveForecastFeatureIds(TimeseriesForecastTask task) {
        if (task == null) return Set.of();
        List<String> targetIds = task.getForecastObjects() != null
                ? new ArrayList<>(task.getForecastObjects()) : List.of();
        Set<String> features = new HashSet<>();
        if (task.getFeatureSequenceIds() != null) {
            features.addAll(task.getFeatureSequenceIds());
        }
        features.addAll(collectFeatureSequenceIds(targetIds));
        return features;
    }

    // ── resolve methods ──────────────────────────────────────────────

    public SemanticContext resolveAnomalyContext(TimeseriesAnomalyTask task) {
        SemanticContext.Builder ctx = SemanticContext.newBuilder();
        if (task == null) {
            return ctx.build();
        }

        List<String> seqIds = task.getSequenceIds() != null
                ? new ArrayList<>(task.getSequenceIds()) : List.of();

        // sequences metadata
        for (String seqId : seqIds) {
            TimeseriesInstanceConfig inst = memoryCache.getInstanceBySequenceId(seqId);
            if (inst != null) {
                ctx.addSequences(toSequenceMetadata(inst, "TARGET"));
            }
        }

        // relations — discovered by sequenceId AND categoryId, expanded to concrete pairs
        ctx.addAllRelations(collectExpandedRelations(new HashSet<>(seqIds)));

        // constraint ids — 前端传入 + auto-discovered
        Set<String> constraintIds = collectConstraintIds(seqIds, task.getConstraintIds());
        ctx.addAllConstraintIds(constraintIds);

        // feature sequences from relations (category-level also expanded)
        for (TimeseriesInstanceConfig srcInst : expandFeatureSequences(seqIds)) {
            ctx.addSequences(toSequenceMetadata(srcInst, "FEATURE"));
        }

        ctx.setKnowledgeVersion(String.valueOf(System.currentTimeMillis()));
        return ctx.build();
    }

    public SemanticContext resolveForecastContext(TimeseriesForecastTask task) {
        SemanticContext.Builder ctx = SemanticContext.newBuilder();
        if (task == null) {
            return ctx.build();
        }

        List<String> targetIds = task.getForecastObjects() != null
                ? new ArrayList<>(task.getForecastObjects()) : List.of();
        List<String> featureIds = task.getFeatureSequenceIds() != null
                ? new ArrayList<>(task.getFeatureSequenceIds()) : new ArrayList<>();

        Set<String> autoFeatures = new HashSet<>(featureIds);
        autoFeatures.addAll(collectFeatureSequenceIds(targetIds));

        // target sequences
        for (String seqId : targetIds) {
            TimeseriesInstanceConfig inst = memoryCache.getInstanceBySequenceId(seqId);
            if (inst != null) {
                ctx.addSequences(toSequenceMetadata(inst, "TARGET"));
            }
        }
        // feature sequences
        for (String seqId : autoFeatures) {
            TimeseriesInstanceConfig inst = memoryCache.getInstanceBySequenceId(seqId);
            if (inst != null) {
                ctx.addSequences(toSequenceMetadata(inst, "FEATURE"));
            }
        }

        // relations — discovered by sequenceId AND categoryId, expanded to concrete pairs
        Set<String> candidateIds = new HashSet<>(targetIds);
        candidateIds.addAll(autoFeatures);
        ctx.addAllRelations(collectExpandedRelations(candidateIds));

        // constraint ids
        Set<String> constraintIds = collectConstraintIds(targetIds, task.getConstraintIds());
        ctx.addAllConstraintIds(constraintIds);

        ctx.setKnowledgeVersion(String.valueOf(System.currentTimeMillis()));
        return ctx.build();
    }

    private SequenceMetadata toSequenceMetadata(TimeseriesInstanceConfig inst, String role) {
        String unit = "";
        if (inst.getCategoryId() != null) {
            unit = memoryCache.getCategory(inst.getCategoryId())
                    .map(c -> c.getDefaultUnit() != null ? c.getDefaultUnit() : "")
                    .orElse("");
        }
        return SequenceMetadata.newBuilder()
                .setSequenceId(inst.getSequenceId() != null ? inst.getSequenceId() : "")
                .setSequenceName(inst.getInstanceName() != null ? inst.getInstanceName() : "")
                .setUnit(unit)
                .setDataType(inst.getDataType() != null ? inst.getDataType() : "")
                .setRole(role)
                .build();
    }

    /**
     * Discover and expand relations for the given sequence IDs.
     * Looks up by both sequenceId AND categoryId.
     * Category-level sources/targets are expanded to concrete sequence pairs,
     * with relationId matching the Core-end format: {relationId}_{source}_{target}.
     */
    private List<SequenceRelation> collectExpandedRelations(Set<String> seqIds) {
        if (seqIds.isEmpty()) return List.of();

        // Lookup keys: sequenceIds + their categoryIds
        Set<String> lookupKeys = new HashSet<>(seqIds);
        for (String seqId : seqIds) {
            TimeseriesInstanceConfig inst = memoryCache.getInstanceBySequenceId(seqId);
            if (inst != null && inst.getCategoryId() != null) {
                lookupKeys.add(inst.getCategoryId());
            }
        }

        List<SequenceRelation> result = new ArrayList<>();
        Set<String> addedExpandedIds = new HashSet<>();

        for (String key : lookupKeys) {
            for (TimeseriesRelation rel : memoryCache.listRelationsByTargetSequenceId(key)) {
                expandRelation(rel, seqIds, addedExpandedIds, result);
            }
            for (TimeseriesRelation rel : memoryCache.listRelationsBySourceSequenceId(key)) {
                expandRelation(rel, seqIds, addedExpandedIds, result);
            }
        }
        return result;
    }

    private void expandRelation(TimeseriesRelation rel, Set<String> taskSeqIds,
            Set<String> addedExpandedIds, List<SequenceRelation> result) {
        if (!isRelationEnabled(rel)) return;

        List<String> srcSeqs = expandIds(rel.getSourceSequences());
        if (srcSeqs.isEmpty()) return;

        List<String> tgtSeqs = expandIds(
                rel.getTargetSequenceId() != null ? List.of(rel.getTargetSequenceId()) : List.of());
        // Only include targets that are in the task's sequences
        tgtSeqs.retainAll(taskSeqIds);
        if (tgtSeqs.isEmpty()) return;

        String baseId = rel.getRelationId() != null ? rel.getRelationId() : "";
        String relType = rel.getRelationType() != null ? rel.getRelationType() : "";
        double confidence = rel.getConfidence() != null ? rel.getConfidence().doubleValue() : 0.0;
        int lagSteps = parseLag(rel.getLagRange());

        for (String src : srcSeqs) {
            for (String tgt : tgtSeqs) {
                if (src.equals(tgt)) continue;
                String expandedId = baseId + "_" + src + "_" + tgt;
                if (!addedExpandedIds.add(expandedId)) continue;
                result.add(SequenceRelation.newBuilder()
                        .setRelationId(expandedId)
                        .setSourceSequenceId(src)
                        .setTargetSequenceId(tgt)
                        .setRelationType(relType)
                        .setLagSteps(lagSteps)
                        .setConfidence(confidence)
                        .build());
            }
        }
    }

    /**
     * Expand category IDs to sequence IDs. If an ID is a category, enumerate
     * all sequences under that category. Otherwise treat as a sequence ID directly.
     */
    private List<String> expandIds(Collection<String> ids) {
        if (ids == null || ids.isEmpty()) return List.of();
        List<String> result = new ArrayList<>();
        for (String id : ids) {
            if (id == null || id.isBlank()) continue;
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

    /**
     * Collect feature sequence IDs from relations targeting the given sequence IDs.
     * Category-level relations are expanded.
     */
    private Set<String> collectFeatureSequenceIds(List<String> targetIds) {
        Set<String> features = new HashSet<>();
        Set<String> lookupKeys = new HashSet<>(targetIds);
        for (String seqId : targetIds) {
            TimeseriesInstanceConfig inst = memoryCache.getInstanceBySequenceId(seqId);
            if (inst != null && inst.getCategoryId() != null) {
                lookupKeys.add(inst.getCategoryId());
            }
        }
        for (String key : lookupKeys) {
            for (TimeseriesRelation rel : memoryCache.listRelationsByTargetSequenceId(key)) {
                if (!isRelationEnabled(rel)) continue;
                if (rel.getSourceSequences() != null) {
                    features.addAll(expandIds(rel.getSourceSequences()));
                }
            }
        }
        return features;
    }

    /**
     * Expand feature sequences and return their InstanceConfig for metadata.
     */
    private List<TimeseriesInstanceConfig> expandFeatureSequences(List<String> targetIds) {
        Set<String> featureIds = collectFeatureSequenceIds(targetIds);
        List<TimeseriesInstanceConfig> result = new ArrayList<>();
        for (String seqId : featureIds) {
            TimeseriesInstanceConfig inst = memoryCache.getInstanceBySequenceId(seqId);
            if (inst != null) {
                result.add(inst);
            }
        }
        return result;
    }

    private int parseLag(String lagRange) {
        if (lagRange == null || lagRange.isBlank()) return 0;
        try {
            return Integer.parseInt(lagRange.trim());
        } catch (NumberFormatException ignored) {
            return 0;
        }
    }

    /**
     * Collect constraint IDs matching the task's sequences.
     * 1. Explicitly specified by the caller
     * 2. Discovered via categoryId lookup
     * 3. Discovered via variableMapping value match (cross-category constraints)
     */
    private Set<String> collectConstraintIds(List<String> seqIds, java.util.Collection<String> explicitIds) {
        Set<String> result = new HashSet<>();
        if (explicitIds != null) {
            result.addAll(explicitIds);
        }

        Set<String> seqIdSet = new HashSet<>(seqIds);
        for (TimeseriesConstraint c : memoryCache.listConstraints()) {
            if (!"ENABLE".equalsIgnoreCase(c.getEffectiveStatus())
                    || !"CONFIRMED".equalsIgnoreCase(c.getConfirmStatus())
                    || c.getConstraintId() == null) {
                continue;
            }
            // match by categoryId
            boolean matched = false;
            for (String seqId : seqIds) {
                TimeseriesInstanceConfig inst = memoryCache.getInstanceBySequenceId(seqId);
                if (inst != null && c.getCategoryId() != null
                        && c.getCategoryId().equals(inst.getCategoryId())) {
                    matched = true;
                    break;
                }
            }
            // match by variableMapping values (cross-category)
            if (!matched && c.getVariableMapping() != null) {
                for (String mappedSeqId : c.getVariableMapping().values()) {
                    if (seqIdSet.contains(mappedSeqId)) {
                        matched = true;
                        break;
                    }
                }
            }
            if (matched) {
                result.add(c.getConstraintId());
            }
        }
        return result;
    }

    private boolean isRelationEnabled(TimeseriesRelation rel) {
        return rel != null && "ENABLE".equalsIgnoreCase(rel.getEffectiveStatus());
    }
}
