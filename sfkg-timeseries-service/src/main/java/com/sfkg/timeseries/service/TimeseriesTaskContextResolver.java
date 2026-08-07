package com.sfkg.timeseries.service;

import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.entity.TimeseriesAnomalyTask;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.entity.TimeseriesForecastTask;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.entity.TimeseriesRelation;
import com.sfkg.timeseries.grpc.SemanticContext;
import com.sfkg.timeseries.grpc.SequenceMetadata;
import com.sfkg.timeseries.grpc.SequenceRelation;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.springframework.stereotype.Component;

@Component
public class TimeseriesTaskContextResolver {

    private final TimeseriesMemoryCache memoryCache;

    public TimeseriesTaskContextResolver(TimeseriesMemoryCache memoryCache) {
        this.memoryCache = memoryCache;
    }

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

        // relations
        Set<String> addedRelationIds = new HashSet<>();
        for (String seqId : seqIds) {
            for (TimeseriesRelation rel : memoryCache.listRelationsByTargetSequenceId(seqId)) {
                if (isRelationEnabled(rel) && addedRelationIds.add(rel.getRelationId())) {
                    ctx.addAllRelations(toProtoRelation(rel));
                }
            }
            for (TimeseriesRelation rel : memoryCache.listRelationsBySourceSequenceId(seqId)) {
                if (isRelationEnabled(rel) && addedRelationIds.add(rel.getRelationId())) {
                    ctx.addAllRelations(toProtoRelation(rel));
                }
            }
        }

        // constraint ids — 前端传入 + auto-discovered
        Set<String> constraintIds = collectConstraintIds(seqIds, task.getConstraintIds());
        ctx.addAllConstraintIds(constraintIds);

        // feature sequences from relations
        for (String seqId : seqIds) {
            for (TimeseriesRelation rel : memoryCache.listRelationsByTargetSequenceId(seqId)) {
                if (!isRelationEnabled(rel)) continue;
                if (rel.getSourceSequences() != null) {
                    for (String srcSeq : rel.getSourceSequences()) {
                        TimeseriesInstanceConfig srcInst = memoryCache.getInstanceBySequenceId(srcSeq);
                        if (srcInst != null) {
                            ctx.addSequences(toSequenceMetadata(srcInst, "FEATURE"));
                        }
                    }
                }
            }
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

        // 自动补 feature ids from relations
        Set<String> autoFeatures = new HashSet<>(featureIds);
        for (String targetId : targetIds) {
            for (TimeseriesRelation rel : memoryCache.listRelationsByTargetSequenceId(targetId)) {
                if (!isRelationEnabled(rel)) continue;
                if (rel.getSourceSequences() != null) {
                    autoFeatures.addAll(rel.getSourceSequences());
                }
            }
        }

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

        // relations
        Set<String> addedRelationIds = new HashSet<>();
        for (String seqId : targetIds) {
            for (TimeseriesRelation rel : memoryCache.listRelationsByTargetSequenceId(seqId)) {
                if (isRelationEnabled(rel) && addedRelationIds.add(rel.getRelationId())) {
                    ctx.addAllRelations(toProtoRelation(rel));
                }
            }
        }
        for (String seqId : autoFeatures) {
            for (TimeseriesRelation rel : memoryCache.listRelationsBySourceSequenceId(seqId)) {
                if (isRelationEnabled(rel) && addedRelationIds.add(rel.getRelationId())) {
                    ctx.addAllRelations(toProtoRelation(rel));
                }
            }
        }

        // constraint ids
        Set<String> constraintIds = collectConstraintIds(targetIds, task.getConstraintIds());
        ctx.addAllConstraintIds(constraintIds);

        ctx.setKnowledgeVersion(String.valueOf(System.currentTimeMillis()));
        return ctx.build();
    }

    private SequenceMetadata toSequenceMetadata(TimeseriesInstanceConfig inst, String role) {
        return SequenceMetadata.newBuilder()
                .setSequenceId(inst.getSequenceId() != null ? inst.getSequenceId() : "")
                .setSequenceName(inst.getInstanceName() != null ? inst.getInstanceName() : "")
                .setDataType(inst.getDataType() != null ? inst.getDataType() : "")
                .setRole(role)
                .build();
    }

    private List<SequenceRelation> toProtoRelation(TimeseriesRelation rel) {
        String relationId = rel.getRelationId() != null ? rel.getRelationId() : "";
        String targetId = rel.getTargetSequenceId() != null ? rel.getTargetSequenceId() : "";
        String relType = rel.getRelationType() != null ? rel.getRelationType() : "";
        double confidence = rel.getConfidence() != null ? rel.getConfidence().doubleValue() : 0.0;
        int lagSteps = 0;
        if (rel.getLagRange() != null) {
            try {
                lagSteps = Integer.parseInt(rel.getLagRange().trim());
            } catch (NumberFormatException ignored) {}
        }

        List<SequenceRelation> result = new ArrayList<>();
        if (rel.getSourceSequences() != null && !rel.getSourceSequences().isEmpty()) {
            for (String src : rel.getSourceSequences()) {
                if (src == null || src.isBlank()) continue;
                result.add(SequenceRelation.newBuilder()
                        .setRelationId(relationId)
                        .setSourceSequenceId(src)
                        .setTargetSequenceId(targetId)
                        .setRelationType(relType)
                        .setLagSteps(lagSteps)
                        .setConfidence(confidence)
                        .build());
            }
        } else {
            result.add(SequenceRelation.newBuilder()
                    .setRelationId(relationId)
                    .setTargetSequenceId(targetId)
                    .setRelationType(relType)
                    .setLagSteps(lagSteps)
                    .setConfidence(confidence)
                    .build());
        }
        return result;
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
