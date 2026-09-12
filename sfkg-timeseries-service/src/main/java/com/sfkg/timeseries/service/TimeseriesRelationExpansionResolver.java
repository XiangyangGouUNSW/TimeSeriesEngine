package com.sfkg.timeseries.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.entity.TimeseriesRelation;

/**
 * 关系展开的唯一实现：类别 ID → 序列 ID、按 {@code deviceInstanceId} 分桶、
 * 生成 {@code {relationId}_{source}_{target}} 形式的实例级 relation_id。
 *
 * <p>S 端要把同一批实例级关系同时下发给两个消费方：
 * <ul>
 *   <li><b>C 端</b>：{@code SyncRelationsRequest} 里的 {@code RuntimeRelationConfig}，用于实时关联检测；</li>
 *   <li><b>P 端</b>：任务 {@code SemanticContext.relations}，用于模型的自变量列先验。</li>
 * </ul>
 *
 * <p>以前这两条路径各写了一份展开逻辑，配对规则（是否按设备分桶）与启用判定
 * （是否要求 {@code CONFIRMED}）都会漂移，导致 P 端可能拿到 C 端根本不存在的
 * relation_id。现在收敛到这里，保证两端逐条关系 ID 完全一致。
 *
 * <p>约束侧的对应实现是 {@link TimeseriesConstraintExpansionResolver}。
 */
@Component
public class TimeseriesRelationExpansionResolver {

    /** 没有 deviceInstanceId 的实例统一归到这个桶。 */
    public static final String DEFAULT_DEVICE = "_default";

    private final TimeseriesMemoryCache memoryCache;

    public TimeseriesRelationExpansionResolver(TimeseriesMemoryCache memoryCache) {
        this.memoryCache = memoryCache;
    }

    /** 一条展开后的实例级关系对；source 与 target 一定属于同一台设备。 */
    public record ExpandedRelationPair(String relationId, String sourceSequenceId, String targetSequenceId) {
    }

    /**
     * 关系是否生效：{@code ENABLE|ENABLED} 且 {@code CONFIRMED}。
     * 与约束侧 {@code isConstraintActive} 同口径 —— C 端下发与 P 端上下文必须用同一判定。
     */
    public boolean isRelationEnabled(TimeseriesRelation relation) {
        return relation != null
                && relation.getRelationId() != null
                && isEffective(relation.getEffectiveStatus())
                && "CONFIRMED".equalsIgnoreCase(relation.getConfirmStatus());
    }

    private boolean isEffective(String status) {
        return "ENABLE".equalsIgnoreCase(status) || "ENABLED".equalsIgnoreCase(status);
    }

    /**
     * 展开成实例级关系对（同设备配对，跳过自环，按 relation_id 去重）。
     *
     * <p>本方法只做「几何」展开，<b>不判断启用状态</b>：C 端下发必须能把停用/未确认的
     * 关系也推过去（带 {@code enabled=false}）才能撤销既有规则，所以是否生效由调用方
     * 用 {@link #isRelationEnabled} 决定。
     *
     * @param allowedTargets 非空时只保留其中的 target（P 端按任务序列过滤）；
     *                       {@code null}/空 = 不限制 target（C 端下发全部）
     */
    public List<ExpandedRelationPair> expand(TimeseriesRelation relation,
                                             Collection<String> allowedTargets) {
        if (relation == null || relation.getRelationId() == null) {
            return List.of();
        }
        String projectId = relation.getProjectId();

        List<String> srcSeqIds = resolveToSequences(projectId, relation.getSourceSequences());
        if (srcSeqIds.isEmpty()) {
            return List.of();
        }
        List<String> tgtSeqIds = resolveToSequences(projectId,
                relation.getTargetSequenceId() != null
                        ? List.of(relation.getTargetSequenceId()) : List.of());
        if (allowedTargets != null && !allowedTargets.isEmpty()) {
            Set<String> allowed = new HashSet<>(allowedTargets);
            tgtSeqIds = tgtSeqIds.stream().filter(allowed::contains).toList();
        }
        if (tgtSeqIds.isEmpty()) {
            return List.of();
        }

        Map<String, List<String>> srcByDevice = groupByDeviceInstanceId(projectId, srcSeqIds);
        Map<String, List<String>> tgtByDevice = groupByDeviceInstanceId(projectId, tgtSeqIds);

        List<ExpandedRelationPair> result = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (Map.Entry<String, List<String>> entry : srcByDevice.entrySet()) {
            // 目标不在同一台设备 → 整台设备不配对（跨设备关系不下发）
            List<String> tgtInDevice = tgtByDevice.getOrDefault(entry.getKey(), List.of());
            if (tgtInDevice.isEmpty()) {
                continue;
            }
            for (String src : entry.getValue()) {
                for (String tgt : tgtInDevice) {
                    if (src.equals(tgt)) {
                        continue;   // 跳过自环
                    }
                    String expandedId = nullToEmpty(relation.getRelationId()) + "_" + src + "_" + tgt;
                    if (seen.add(expandedId)) {
                        result.add(new ExpandedRelationPair(expandedId, src, tgt));
                    }
                }
            }
        }
        return result;
    }

    /** 类别 ID → 该类别下所有序列 ID；不是类别就当作序列 ID 原样保留。 */
    public List<String> resolveToSequences(String projectId, Collection<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        for (String id : ids) {
            if (id == null || id.isBlank()) {
                continue;
            }
            if (memoryCache.getCategory(projectId, id).isPresent()) {
                for (TimeseriesInstanceConfig inst : memoryCache.listInstanceConfigs()) {
                    if (Objects.equals(projectId, inst.getProjectId())
                            && id.equals(inst.getCategoryId())
                            && inst.getSequenceId() != null) {
                        result.add(inst.getSequenceId());
                    }
                }
            } else {
                result.add(id);
            }
        }
        return result;
    }

    /** 按 deviceInstanceId 分桶；缺该字段的实例归入 {@link #DEFAULT_DEVICE}。 */
    public Map<String, List<String>> groupByDeviceInstanceId(String projectId, List<String> seqIds) {
        Map<String, List<String>> map = new LinkedHashMap<>();
        if (seqIds == null) {
            return map;
        }
        for (String seqId : seqIds) {
            TimeseriesInstanceConfig inst = memoryCache.getInstanceBySequenceId(projectId, seqId);
            if (inst == null && projectId == null) {
                inst = memoryCache.getInstanceBySequenceId(seqId);
            }
            String deviceId = inst != null && inst.getDeviceInstanceId() != null
                    ? inst.getDeviceInstanceId()
                    : DEFAULT_DEVICE;
            map.computeIfAbsent(deviceId, key -> new ArrayList<>()).add(seqId);
        }
        return map;
    }

    private String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
