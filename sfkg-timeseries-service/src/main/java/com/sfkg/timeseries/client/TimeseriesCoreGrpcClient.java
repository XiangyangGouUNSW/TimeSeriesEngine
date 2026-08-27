package com.sfkg.timeseries.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.config.GrpcClientProperties;
import com.sfkg.timeseries.dto.DerivedSeriesConfigSaveRequest;
import com.sfkg.timeseries.dto.DerivedSeriesConfigSaveRequest.DerivedExpressionDTO;
import com.sfkg.timeseries.dto.DerivedSeriesConfigSaveRequest.LinearTermDTO;
import com.sfkg.timeseries.dto.HistoryDataQueryRequest;
import com.sfkg.timeseries.dto.SyncResult;
import com.sfkg.timeseries.dto.TimeseriesDataSaveRequest;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.entity.TimeseriesDataPoint;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.entity.TimeseriesRelation;
import com.sfkg.timeseries.grpc.AlignWindowDataRequest;
import com.sfkg.timeseries.grpc.AlignWindowDataResponse;
import com.sfkg.timeseries.grpc.AlignedWindowData;
import com.sfkg.timeseries.grpc.AlignmentConfig;
import com.sfkg.timeseries.grpc.ComputeStatisticsRequest;
import com.sfkg.timeseries.grpc.ComputeStatisticsResponse;
import com.sfkg.timeseries.grpc.ConstraintAggregation;
import com.sfkg.timeseries.grpc.ConstraintRule;
import com.sfkg.timeseries.grpc.ConstraintTerm;
import com.sfkg.timeseries.grpc.DerivedBinaryExpression;
import com.sfkg.timeseries.grpc.DerivedExpression;
import com.sfkg.timeseries.grpc.DerivedOperator;
import com.sfkg.timeseries.grpc.DerivedSeriesConfig;
import com.sfkg.timeseries.grpc.HistoryOverview;
import com.sfkg.timeseries.grpc.IngestDataRequest;
import com.sfkg.timeseries.grpc.IngestDataResponse;
import com.sfkg.timeseries.grpc.LinearCombinationConfig;
import com.sfkg.timeseries.grpc.LinearTerm;
import com.sfkg.timeseries.grpc.OperationCode;
import com.sfkg.timeseries.grpc.OperationResult;
import com.sfkg.timeseries.grpc.QueryHistoryDataRequest;
import com.sfkg.timeseries.grpc.QueryHistoryDataResponse;
import com.sfkg.timeseries.grpc.QueryHistoryOverviewRequest;
import com.sfkg.timeseries.grpc.QueryHistoryOverviewResponse;
import com.sfkg.timeseries.grpc.QueryWindowDataRequest;
import com.sfkg.timeseries.grpc.QueryWindowDataResponse;
import com.sfkg.timeseries.grpc.RawTimeseriesPoint;
import com.sfkg.timeseries.grpc.RelationLagRange;
import com.sfkg.timeseries.grpc.RuntimeConstraintConfig;
import com.sfkg.timeseries.grpc.RuntimeInstanceConfig;
import com.sfkg.timeseries.grpc.RuntimeRelationConfig;
import com.sfkg.timeseries.grpc.RuntimeRelationSource;
import com.sfkg.timeseries.grpc.RuntimeWindowConfig;
import com.sfkg.timeseries.grpc.SequenceAlignmentConfig;
import com.sfkg.timeseries.grpc.SeriesKind;
import com.sfkg.timeseries.grpc.SyncConfigResponse;
import com.sfkg.timeseries.grpc.SyncConstraintsRequest;
import com.sfkg.timeseries.grpc.SyncDerivedSeriesConfigsRequest;
import com.sfkg.timeseries.grpc.SyncInstanceConfigsRequest;
import com.sfkg.timeseries.grpc.SyncRelationsRequest;
import com.sfkg.timeseries.grpc.SyncWindowConfigRequest;
import com.sfkg.timeseries.grpc.TimeseriesCoreServiceGrpc;
import com.sfkg.timeseries.grpc.TimeseriesIngestData;
import com.sfkg.timeseries.grpc.TimeseriesValue;
import com.sfkg.timeseries.grpc.VariableRole;
import com.sfkg.timeseries.grpc.WindowData;
import com.sfkg.timeseries.service.TimeseriesConstraintExpansionResolver;
import com.sfkg.timeseries.service.TimeseriesConstraintExpansionResolver.ExpandedConstraintRule;
import com.sfkg.timeseries.common.ProjectIdValidator;
import com.sfkg.timeseries.vo.HistoryDataVO;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class TimeseriesCoreGrpcClient {

    private static final Logger LOG = LoggerFactory.getLogger(TimeseriesCoreGrpcClient.class);
    private static final String SERVICE_NAME = "timeseries-core";

    private final GrpcClientProperties grpcClientProperties;
    private final ObjectMapper objectMapper;
    private final TimeseriesMemoryCache memoryCache;
    private final GrpcChannelRegistry channelRegistry;
    private final TimeseriesConstraintExpansionResolver constraintExpansionResolver;

    public TimeseriesCoreGrpcClient(GrpcClientProperties grpcClientProperties, ObjectMapper objectMapper,
                                    TimeseriesMemoryCache memoryCache,
                                    GrpcChannelRegistry channelRegistry,
                                    TimeseriesConstraintExpansionResolver constraintExpansionResolver) {
        this.grpcClientProperties = grpcClientProperties;
        this.objectMapper = objectMapper;
        this.memoryCache = memoryCache;
        this.channelRegistry = channelRegistry;
        this.constraintExpansionResolver = constraintExpansionResolver;
    }

    // ── instance config ────────────────────────────────────────────────

    public SyncResult syncInstanceConfig(TimeseriesInstanceConfig config) {
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return notConfigured("syncInstanceConfigs");
        }
        if (config == null) {
            return SyncResult.fail("config is null");
        }
        RuntimeInstanceConfig item = RuntimeInstanceConfig.newBuilder()
                .setSequenceId(nullToEmpty(config.getSequenceId()))
                .setDataSourceId(nullToEmpty(config.getDataSourceId()))
                .setExternalSequenceId(nullToEmpty(config.getExternalSequenceId()))
                .setCategoryId(nullToEmpty(config.getCategoryId()))
                .setDataType(nullToEmpty(config.getDataType()).toLowerCase())
                .setSeriesKind(toSeriesKind(config.getSeriesKind()))
                .setProjectId(nullToEmpty(config.getProjectId()))
                .build();
        SyncInstanceConfigsRequest req = SyncInstanceConfigsRequest.newBuilder()
                .addItems(item)
                .setProjectId(nullToEmpty(config.getProjectId()))
                .build();
        LOG.info("[{}] -> syncInstanceConfigs sequenceId={} at {}", SERVICE_NAME, config.getSequenceId(), address);
        return callCoreSync(address, stub -> stub.syncInstanceConfigs(req), "syncInstanceConfigs");
    }

    public SyncResult syncInstanceConfigs(List<TimeseriesInstanceConfig> configs) {
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return notConfigured("syncInstanceConfigs");
        }
        if (configs == null || configs.isEmpty()) {
            return SyncResult.success();
        }
        SyncInstanceConfigsRequest.Builder reqBuilder = SyncInstanceConfigsRequest.newBuilder();
        for (TimeseriesInstanceConfig config : configs) {
            reqBuilder.addItems(RuntimeInstanceConfig.newBuilder()
                    .setSequenceId(nullToEmpty(config.getSequenceId()))
                    .setDataSourceId(nullToEmpty(config.getDataSourceId()))
                    .setExternalSequenceId(nullToEmpty(config.getExternalSequenceId()))
                    .setCategoryId(nullToEmpty(config.getCategoryId()))
                    .setDataType(nullToEmpty(config.getDataType()).toLowerCase())
                    .setSeriesKind(toSeriesKind(config.getSeriesKind()))
                    .setProjectId(nullToEmpty(config.getProjectId()))
                    .build());
        }
        if (configs.get(0).getProjectId() != null) {
            reqBuilder.setProjectId(configs.get(0).getProjectId());
        }
        LOG.info("[{}] -> syncInstanceConfigs count={} at {}", SERVICE_NAME, configs.size(), address);
        return callCoreSync(address, stub -> stub.syncInstanceConfigs(reqBuilder.build()), "syncInstanceConfigs");
    }

    // ── constraint config ──────────────────────────────────────────────

    public SyncResult syncConstraintConfig(TimeseriesConstraint constraint) {
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return notConfigured("syncConstraints");
        }
        if (constraint == null) {
            return SyncResult.fail("constraint is null");
        }

        boolean enabled = isEnabled(constraint.getEffectiveStatus());
        List<RuntimeConstraintConfig> items = buildRuntimeConstraintConfigs(constraint, enabled);

        SyncConstraintsRequest req = SyncConstraintsRequest.newBuilder()
                .addAllItems(items)
                .setProjectId(nullToEmpty(constraint.getProjectId()))
                .build();
        LOG.info("[{}] -> syncConstraints constraintId={} expanded to {} rules at {}",
                SERVICE_NAME, constraint.getConstraintId(), items.size(), address);
        return callCoreSync(address, stub -> stub.syncConstraints(req), "syncConstraints");
    }

    /**
     * 批量同步多条约束到 Core：所有规则的 RuntimeConstraintConfig
     * 汇总到同一个 SyncConstraintsRequest，一次 RPC 原子送达。
     * 用于 OR 组批量创建（同组规则同时到达，避免中途求值产生半组状态）。
     */
    public SyncResult syncConstraintConfigs(List<TimeseriesConstraint> constraints) {
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return notConfigured("syncConstraints");
        }
        if (constraints == null || constraints.isEmpty()) {
            return SyncResult.success();
        }
        SyncConstraintsRequest.Builder reqBuilder = SyncConstraintsRequest.newBuilder();
        int ruleCount = 0;
        for (TimeseriesConstraint constraint : constraints) {
            List<RuntimeConstraintConfig> items = buildRuntimeConstraintConfigs(
                    constraint, isEnabled(constraint.getEffectiveStatus()));
            reqBuilder.addAllItems(items);
            ruleCount += items.size();
        }
        TimeseriesConstraint first = constraints.get(0);
        reqBuilder.setProjectId(nullToEmpty(first.getProjectId()));
        LOG.info("[{}] -> syncConstraints batch constraints={} rules={} at {}",
                SERVICE_NAME, constraints.size(), ruleCount, address);
        return callCoreSync(address, stub -> stub.syncConstraints(reqBuilder.build()), "syncConstraints");
    }

    private List<RuntimeConstraintConfig> buildRuntimeConstraintConfigs(
            TimeseriesConstraint constraint, boolean enabled) {
        List<ExpandedConstraintRule> expandedRules = constraintExpansionResolver.expandConstraint(constraint);
        List<RuntimeConstraintConfig> items = new ArrayList<>();
        for (ExpandedConstraintRule expandedRule : expandedRules) {
            ConstraintRule.Builder rb = ConstraintRule.newBuilder()
                    .setConstraintId(expandedRule.constraintId())
                    .setProjectId(nullToEmpty(constraint.getProjectId()))
                    .putAllVariableMapping(expandedRule.variableMapping());
            // optional 语义：null = 未设置（不再用 ±Double.MAX_VALUE 哨兵值）
            if (constraint.getLowerBound() != null) {
                rb.setLowerBound(constraint.getLowerBound());
            }
            if (constraint.getUpperBound() != null) {
                rb.setUpperBound(constraint.getUpperBound());
            }
            // 展开后的组 ID 已带序列后缀：同设备规则共享同组（组内 OR），
            // 不同设备规则组 ID 不同（组间独立），类别级展开不会跨设备 OR。
            if (expandedRule.orGroupId() != null && !expandedRule.orGroupId().isBlank()) {
                rb.setOrGroupId(expandedRule.orGroupId());
            }
            if (constraint.getTerms() != null) {
                for (TimeseriesConstraint.ConstraintTermItem term : constraint.getTerms()) {
                    rb.addTerms(ConstraintTerm.newBuilder()
                            .setVariable(nullToEmpty(term.getVariable()))
                            .setCoefficient(term.getCoefficient() != null ? term.getCoefficient() : 0.0)
                            .setSampleOffset(term.getSampleOffset() != null ? term.getSampleOffset() : 0L)
                            .setAggregation(toConstraintAggregation(term.getAggregation()))
                            .build());
                }
            }
            items.add(RuntimeConstraintConfig.newBuilder()
                    .setRule(rb.build())
                    .setEnabled(enabled)
                    .setProjectId(nullToEmpty(constraint.getProjectId()))
                    .build());
        }
        return items;
    }

    /** 将字符串聚合方式映射为 proto 枚举；空值/未知值回退到 SAMPLE。 */
    private ConstraintAggregation toConstraintAggregation(String aggregation) {
        if (aggregation == null || aggregation.isBlank()) {
            return ConstraintAggregation.CONSTRAINT_AGGREGATION_SAMPLE;
        }
        return switch (aggregation.trim().toUpperCase()) {
            case "AVERAGE" -> ConstraintAggregation.CONSTRAINT_AGGREGATION_AVERAGE;
            case "MAXIMUM" -> ConstraintAggregation.CONSTRAINT_AGGREGATION_MAXIMUM;
            case "MINIMUM" -> ConstraintAggregation.CONSTRAINT_AGGREGATION_MINIMUM;
            default -> ConstraintAggregation.CONSTRAINT_AGGREGATION_SAMPLE;
        };
    }

    private boolean isEnabled(String status) {
        return "ENABLE".equalsIgnoreCase(status) || "ENABLED".equalsIgnoreCase(status);
    }

    // ── relation config ────────────────────────────────────────────────

    public SyncResult syncRelationConfig(TimeseriesRelation relation) {
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return notConfigured("syncRelations");
        }
        if (relation == null) {
            return SyncResult.fail("relation is null");
        }

        // Resolve source/target: category IDs → sequence IDs, grouped by deviceInstanceId
        List<String> srcSeqIds = resolveToSequences(relation.getProjectId(), relation.getSourceSequences());
        List<String> tgtSeqIds = resolveToSequences(relation.getProjectId(),
                relation.getTargetSequenceId() != null ? List.of(relation.getTargetSequenceId()) : List.of());

        // Group by deviceInstanceId — only pair sequences on the same device
        Map<String, List<String>> srcByDevice = groupByDeviceInstanceId(relation.getProjectId(), srcSeqIds);
        Map<String, List<String>> tgtByDevice = groupByDeviceInstanceId(relation.getProjectId(), tgtSeqIds);

        SyncRelationsRequest.Builder reqBuilder = SyncRelationsRequest.newBuilder()
                .setProjectId(nullToEmpty(relation.getProjectId()));
        int count = 0;
        for (Map.Entry<String, List<String>> entry : srcByDevice.entrySet()) {
            String deviceId = entry.getKey();
            List<String> tgtInDevice = tgtByDevice.getOrDefault(deviceId, List.of());
            if (tgtInDevice.isEmpty()) continue;

            for (String src : entry.getValue()) {
                for (String tgt : tgtInDevice) {
                    if (src.equals(tgt)) continue; // skip self-relation
                    RuntimeRelationConfig.Builder rb = RuntimeRelationConfig.newBuilder()
                            .setRelationId(nullToEmpty(relation.getRelationId()) + "_" + src + "_" + tgt)
                            .setTargetSequenceId(nullToEmpty(tgt))
                            .setRelationType(nullToEmpty(relation.getRelationType()).toLowerCase())
                            .setConfidence(relation.getConfidence() != null ? relation.getConfidence().doubleValue() : 0.0)
                            .setProjectId(nullToEmpty(relation.getProjectId()))
                            .setEnabled("ENABLE".equalsIgnoreCase(relation.getEffectiveStatus()));
                    // 每个展开后的 pair 只有一个 source，权重恒为 1.0
                    RuntimeRelationSource source = buildRelationSource(src, relation.getLagRange());
                    if (source != null) {
                        rb.addSources(source);
                    }
                    reqBuilder.addItems(rb.build());
                    count++;
                }
            }
        }

        if (count == 0) {
            LOG.warn("[{}] syncRelations relationId={}: no sequence pairs resolved, skip",
                    SERVICE_NAME, relation.getRelationId());
            return SyncResult.success();
        }

        LOG.info("[{}] -> syncRelations relationId={} expanded to {} pairs at {}",
                SERVICE_NAME, relation.getRelationId(), count, address);
        return callCoreSync(address, stub -> stub.syncRelations(reqBuilder.build()), "syncRelations");
    }

    // ── window config ──────────────────────────────────────────────────

    public SyncResult syncWindowConfig(long windowSizeMs) {
        throw new IllegalArgumentException("projectId is required for syncWindowConfig");
    }

    public SyncResult syncWindowConfig(String projectId, long windowSizeMs) {
        projectId = ProjectIdValidator.require(projectId);
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return notConfigured("syncWindowConfig");
        }
        SyncWindowConfigRequest req = SyncWindowConfigRequest.newBuilder()
                .setConfig(RuntimeWindowConfig.newBuilder()
                        .setWindowSize(windowSizeMs)
                        .setProjectId(nullToEmpty(projectId))
                        .build())
                .setProjectId(nullToEmpty(projectId))
                .build();
        LOG.info("[{}] -> syncWindowConfig windowSize={}ms at {}", SERVICE_NAME, windowSizeMs, address);
        return callCoreSync(address, stub -> stub.syncWindowConfig(req), "syncWindowConfig");
    }

    // ── derived series config ──────────────────────────────────────────

    public SyncResult syncDerivedSeriesConfigs(DerivedSeriesConfigSaveRequest request) {
        if (request == null) {
            return SyncResult.fail("derived series request is null");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return notConfigured("syncDerivedSeriesConfigs");
        }
        if (request == null || request.getItems() == null || request.getItems().isEmpty()) {
            return SyncResult.fail("no derived series configs");
        }
        SyncDerivedSeriesConfigsRequest.Builder b = SyncDerivedSeriesConfigsRequest.newBuilder()
                .setProjectId(nullToEmpty(request.getProjectId()));
        for (DerivedSeriesConfigSaveRequest.DerivedSeriesConfigItem item : request.getItems()) {
            DerivedSeriesConfig.Builder cb = DerivedSeriesConfig.newBuilder()
                    .setDerivedSequenceId(nullToEmpty(item.getDerivedSequenceId()))
                    .setEnabled(item.isEnabled())
                    .setProjectId(nullToEmpty(request.getProjectId()));
            if (item.getLinearCombination() != null) {
                LinearCombinationConfig.Builder lc = LinearCombinationConfig.newBuilder()
                        .setBias(item.getLinearCombination().getBias() != null
                                ? item.getLinearCombination().getBias() : 0.0);
                if (item.getLinearCombination().getTerms() != null) {
                    for (LinearTermDTO term : item.getLinearCombination().getTerms()) {
                        lc.addTerms(LinearTerm.newBuilder()
                                .setSequenceId(nullToEmpty(term.getSequenceId()))
                                .setCoefficient(term.getCoefficient() != null ? term.getCoefficient() : 0.0)
                                .build());
                    }
                }
                cb.setLinearCombination(lc.build());
            } else if (item.getExpression() != null) {
                cb.setExpression(buildDerivedExpression(item.getExpression()));
            }
            b.addItems(cb.build());
        }
        LOG.info("[{}] -> syncDerivedSeriesConfigs count={} at {}", SERVICE_NAME, b.getItemsCount(), address);
        return callCoreSync(address, stub -> stub.syncDerivedSeriesConfigs(b.build()), "syncDerivedSeriesConfigs");
    }

    private DerivedExpression buildDerivedExpression(DerivedExpressionDTO dto) {
        DerivedExpression.Builder b = DerivedExpression.newBuilder();
        if (dto.getSequenceId() != null) {
            b.setSequenceId(dto.getSequenceId());
        } else if (dto.getConstant() != null) {
            b.setConstant(dto.getConstant());
        } else if (dto.getBinary() != null) {
            b.setBinary(DerivedBinaryExpression.newBuilder()
                    .setOperator(toDerivedOperator(dto.getBinary().getOperator()))
                    .setLeft(buildDerivedExpression(dto.getBinary().getLeft()))
                    .setRight(buildDerivedExpression(dto.getBinary().getRight()))
                    .build());
        }
        return b.build();
    }

    private DerivedOperator toDerivedOperator(String op) {
        if (op == null) return DerivedOperator.DERIVED_OPERATOR_UNSPECIFIED;
        return switch (op.toUpperCase()) {
            case "ADD" -> DerivedOperator.DERIVED_OPERATOR_ADD;
            case "SUBTRACT" -> DerivedOperator.DERIVED_OPERATOR_SUBTRACT;
            case "MULTIPLY" -> DerivedOperator.DERIVED_OPERATOR_MULTIPLY;
            case "DIVIDE" -> DerivedOperator.DERIVED_OPERATOR_DIVIDE;
            default -> DerivedOperator.DERIVED_OPERATOR_UNSPECIFIED;
        };
    }

    // ── relation config helpers ────────────────────────────────────────
    private List<String> resolveToSequences(Collection<String> ids) {
        return resolveToSequences(null, ids);
    }

    private List<String> resolveToSequences(String projectId, Collection<String> ids) {
        if (ids == null || ids.isEmpty()) return List.of();
        List<String> result = new ArrayList<>();
        for (String id : ids) {
            if (id == null || id.isBlank()) continue;
            // Check if it's a category ID
            if (memoryCache.getCategory(projectId, id).isPresent()) {
                // Expand category → all sequence IDs
                for (TimeseriesInstanceConfig inst : memoryCache.listInstanceConfigs()) {
                    if (java.util.Objects.equals(projectId, inst.getProjectId())
                            && id.equals(inst.getCategoryId()) && inst.getSequenceId() != null) {
                        result.add(inst.getSequenceId());
                    }
                }
            } else {
                // Treat as sequence ID directly
                result.add(id);
            }
        }
        return result;
    }

    private Map<String, List<String>> groupByDeviceInstanceId(List<String> seqIds) {
        return groupByDeviceInstanceId(null, seqIds);
    }

    private Map<String, List<String>> groupByDeviceInstanceId(String projectId, List<String> seqIds) {
        Map<String, List<String>> map = new LinkedHashMap<>();
        for (String seqId : seqIds) {
            TimeseriesInstanceConfig inst = memoryCache.getInstanceBySequenceId(projectId, seqId);
            if (inst == null && projectId == null) {
                inst = memoryCache.getInstanceBySequenceId(seqId);
            }
            String deviceId = inst != null && inst.getDeviceInstanceId() != null ? inst.getDeviceInstanceId() : "_default";
            map.computeIfAbsent(deviceId, k -> new ArrayList<>()).add(seqId);
        }
        return map;
    }

    /**
     * Build a {@link RuntimeRelationSource} for one source sequence.
     * <ul>
     *   <li>{@code "5"}          → {@code fixed_lag = 5}（ms）</li>
     *   <li>{@code "5m" / "1h"}  → {@code fixed_lag} 换算为毫秒（m/h/d → ×60000/×3600000/×86400000）</li>
     *   <li>{@code "0m-10m"}     → {@code lag_range { min, max }}（毫秒；注意 Core 端目前仅支持 fixed_lag）</li>
     *   <li>{@code null / blank} → 无滞后约束，仅带 source + weight</li>
     * </ul>
     * Returns {@code null} only when the lag range is present but malformed.
     */
    private RuntimeRelationSource buildRelationSource(String srcSeqId, String lagRange) {
        RuntimeRelationSource.Builder sb = RuntimeRelationSource.newBuilder()
                .setSourceSequenceId(nullToEmpty(srcSeqId))
                // 类别展开后每个 pair 只有一个 source，其线性组合权重恒为 1.0
                .setWeight(1.0);

        if (lagRange == null || lagRange.isBlank()) {
            return sb.build();
        }

        String trimmed = lagRange.trim();
        int dash = trimmed.indexOf('-');
        if (dash >= 0) {
            // 范围格式："0m-10m" / "0-10"
            Long min = parseLagNumber(trimmed.substring(0, dash));
            Long max = parseLagNumber(trimmed.substring(dash + 1));
            if (min == null || max == null) {
                LOG.warn("[{}] syncRelations malformed lagRange '{}', skip source {}",
                        SERVICE_NAME, lagRange, srcSeqId);
                return null;
            }
            sb.setLagRange(RelationLagRange.newBuilder().setMin(min).setMax(max).build());
        } else {
            // 固定滞后："5"
            Long fixed = parseLagNumber(trimmed);
            if (fixed == null) {
                LOG.warn("[{}] syncRelations malformed lagRange '{}', skip source {}",
                        SERVICE_NAME, lagRange, srcSeqId);
                return null;
            }
            sb.setFixedLag(fixed);
        }
        return sb.build();
    }

    /**
     * Parse a lag value to milliseconds. Accepts {@code "5"} (already ms), or
     * an optional unit suffix ({@code m}/{@code h}/{@code d} → minutes/hours/
     * days) converted to milliseconds, because Core interprets lags as
     * millisecond timestamp offsets. Returns {@code null} when malformed.
     */
    private static Long parseLagNumber(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        long multiplier = 1L;
        String num = trimmed;
        switch (trimmed.charAt(trimmed.length() - 1)) {
            case 'm' -> { multiplier = 60L * 1000L; num = trimmed.substring(0, trimmed.length() - 1); }
            case 'h' -> { multiplier = 60L * 60L * 1000L; num = trimmed.substring(0, trimmed.length() - 1); }
            case 'd' -> { multiplier = 24L * 60L * 60L * 1000L; num = trimmed.substring(0, trimmed.length() - 1); }
            default -> { multiplier = 1L; num = trimmed; }
        }
        if (num.isEmpty()) {
            return null;
        }
        try {
            return Long.parseLong(num) * multiplier;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private SeriesKind toSeriesKind(String seriesKind) {
        if (seriesKind == null || seriesKind.isBlank()) {
            return SeriesKind.SERIES_KIND_UNSPECIFIED;
        }
        return switch (seriesKind.toUpperCase()) {
            case "CONTINUOUS" -> SeriesKind.SERIES_KIND_CONTINUOUS;
            case "DISCRETE" -> SeriesKind.SERIES_KIND_DISCRETE;
            case "CATEGORICAL" -> SeriesKind.SERIES_KIND_CATEGORICAL;
            default -> SeriesKind.SERIES_KIND_UNSPECIFIED;
        };
    }

    // ── timeseries data ingest ─────────────────────────────────────────

    public SyncResult ingestData(TimeseriesDataSaveRequest request) {
        if (request == null || request.getPoints() == null || request.getPoints().isEmpty()) {
            return SyncResult.fail("no points to ingest");
        }
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return notConfigured("ingestData");
        }

        IngestDataRequest.Builder reqBuilder = IngestDataRequest.newBuilder();
        reqBuilder.setProjectId(nullToEmpty(request.getProjectId()));
        if (request.getReturnResolvedData() != null) {
            reqBuilder.setReturnResolvedData(request.getReturnResolvedData());
        }
        for (TimeseriesDataSaveRequest.IngestPointDTO p : request.getPoints()) {
            TimeseriesIngestData.Builder pointBuilder = TimeseriesIngestData.newBuilder()
                    .setDataSourceId(nullToEmpty(p.getDataSourceId()))
                    .setExternalSequenceId(nullToEmpty(p.getExternalSequenceId()))
                    .setTime(p.getTime() != null ? p.getTime() : 0L);
            if (p.getSequenceId() != null) {
                pointBuilder.setSequenceId(p.getSequenceId());
            }
            pointBuilder.setProjectId(nullToEmpty(
                    p.getProjectId() != null ? p.getProjectId() : request.getProjectId()));
            pointBuilder.setValue(buildTimeseriesValue(p));
            reqBuilder.addPoints(pointBuilder.build());
        }
        LOG.info("[{}] -> ingestData points={} at {}", SERVICE_NAME, request.getPoints().size(), address);
        return callCoreIngest(address, reqBuilder.build());
    }

    private TimeseriesValue buildTimeseriesValue(TimeseriesDataSaveRequest.IngestPointDTO p) {
        TimeseriesValue.Builder vb = TimeseriesValue.newBuilder();
        if (p.getDoubleValue() != null) {
            vb.setDoubleValue(p.getDoubleValue());
        } else if (p.getInt64Value() != null) {
            vb.setInt64Value(p.getInt64Value());
        } else if (p.getBoolValue() != null) {
            vb.setBoolValue(p.getBoolValue());
        } else if (p.getStringValue() != null) {
            vb.setStringValue(p.getStringValue());
        } else {
            vb.setDoubleValue(0.0);
        }
        return vb.build();
    }

    private SyncResult callCoreIngest(String address, IngestDataRequest req) {
        ManagedChannel channel = channelRegistry.getChannel(address);
        try {
            IngestDataResponse resp = TimeseriesCoreServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .ingestData(req);
            OperationResult op = resp.getOperation();
            boolean success = op.getCode() == OperationCode.OPERATION_CODE_OK
                    || op.getCode() == OperationCode.OPERATION_CODE_PARTIAL_SUCCESS;
            LOG.info("[{}] <- ingestData code={} success={} failed={} msg={}",
                    SERVICE_NAME, op.getCode(), op.getSuccessCount(), op.getFailedCount(), op.getMessage());
            return SyncResult.of(success, op.getMessage());
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] <- ingestData FAILED: code={} desc={}", SERVICE_NAME, e.getStatus().getCode(), e.getStatus().getDescription());
            return SyncResult.fail(e.getStatus().getDescription());
        }
    }

    // ── history data query ─────────────────────────────────────────────

    public HistoryDataVO queryHistoryData(HistoryDataQueryRequest request) {
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            LOG.warn("[{}] queryHistoryData skipped: address not configured", SERVICE_NAME);
            return new HistoryDataVO();
        }
        if (request == null) {
            return new HistoryDataVO();
        }
        QueryHistoryDataRequest.Builder reqBuilder = QueryHistoryDataRequest.newBuilder();
        reqBuilder.addAllSequenceIds(resolveQuerySequenceIds(request));
        if (request.getStartTime() != null) {
            reqBuilder.setStartTime(request.getStartTime().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli());
        }
        if (request.getEndTime() != null) {
            reqBuilder.setEndTime(request.getEndTime().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli());
        }
        if (request.getGranularity() != null) {
            reqBuilder.setGranularity(request.getGranularity());
        }
        reqBuilder.setProjectId(nullToEmpty(request.getProjectId()));
        QueryHistoryDataRequest req = reqBuilder.build();
        LOG.info("[{}] -> queryHistoryData seqs={} at {}", SERVICE_NAME, req.getSequenceIdsList(), address);

        ManagedChannel channel = channelRegistry.getChannel(address);
        try {
            QueryHistoryDataResponse resp = TimeseriesCoreServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .queryHistoryData(req);
            OperationResult op = resp.getOperation();
            LOG.info("[{}] <- queryHistoryData code={} points={}",
                    SERVICE_NAME, op.getCode(), resp.getData().getPointsCount());

            HistoryDataVO vo = new HistoryDataVO();
            vo.setProjectId(request.getProjectId());
            if (resp.hasData() && resp.getData().getPointsCount() > 0) {
                String firstSeq = resp.getData().getPoints(0).getSequenceId();
                vo.setSequenceId(firstSeq);
                vo.setPoints(resp.getData().getPointsList().stream()
                        .map(this::toDataPoint)
                        .collect(java.util.stream.Collectors.toList()));
            }
            return vo;
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] queryHistoryData failed: code={} desc={}", SERVICE_NAME, e.getStatus().getCode(), e.getStatus().getDescription());
            return new HistoryDataVO();
        }
    }

    private TimeseriesDataPoint toDataPoint(RawTimeseriesPoint p) {
        TimeseriesDataPoint dp = new TimeseriesDataPoint();
        dp.setProjectId(p.getProjectId());
        dp.setSequenceId(p.getSequenceId());
        dp.setTimestamp(p.getTime() > 0
                ? java.time.LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(p.getTime()), java.time.ZoneId.systemDefault())
                : null);
        if (p.hasValue()) {
            TimeseriesValue v = p.getValue();
            switch (v.getKindCase()) {
                case DOUBLE_VALUE -> dp.setValue(java.math.BigDecimal.valueOf(v.getDoubleValue()));
                case INT64_VALUE -> dp.setValue(java.math.BigDecimal.valueOf(v.getInt64Value()));
                case BOOL_VALUE -> dp.setValue(v.getBoolValue() ? java.math.BigDecimal.ONE : java.math.BigDecimal.ZERO);
                case STRING_VALUE -> {
                    try { dp.setValue(new java.math.BigDecimal(v.getStringValue())); } catch (Exception ignored) {}
                }
                default -> {}
            }
        }
        return dp;
    }

    // ── history overview ──────────────────────────────────────────────

    public Map<String, Object> queryHistoryOverview(HistoryDataQueryRequest request) {
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return Map.of("error", "core address not configured");
        }
        QueryHistoryOverviewRequest.Builder b = QueryHistoryOverviewRequest.newBuilder();
        b.addAllSequenceIds(resolveQuerySequenceIds(request));
        if (request != null && request.getStartTime() != null) {
            b.setStartTime(request.getStartTime().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli());
        }
        if (request != null && request.getEndTime() != null) {
            b.setEndTime(request.getEndTime().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli());
        }
        if (request != null) {
            b.setProjectId(nullToEmpty(request.getProjectId()));
        }
        LOG.info("[{}] -> queryHistoryOverview at {}", SERVICE_NAME, address);
        ManagedChannel channel = channelRegistry.getChannel(address);
        try {
            QueryHistoryOverviewResponse resp = TimeseriesCoreServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .queryHistoryOverview(b.build());
            HistoryOverview overview = resp.getOverview();
            Map<String, Object> result = new java.util.LinkedHashMap<>();
            result.put("totalPointCount", overview.getTotalPointCount());
            result.put("sequenceCount", overview.getSequenceCount());
            result.put("columnNames", overview.getColumnNamesList());
            result.put("firstTime", overview.hasFirstTime() ? overview.getFirstTime() : null);
            result.put("lastTime", overview.hasLastTime() ? overview.getLastTime() : null);
            result.put("series", overview.getSeriesList().stream().map(s -> {
                Map<String, Object> m = new java.util.LinkedHashMap<>();
                m.put("sequenceId", s.getSequenceId());
                m.put("pointCount", s.getPointCount());
                m.put("firstTime", s.hasFirstTime() ? s.getFirstTime() : null);
                m.put("lastTime", s.hasLastTime() ? s.getLastTime() : null);
                return m;
            }).collect(java.util.stream.Collectors.toList()));
            LOG.info("[{}] <- queryHistoryOverview total={} seqs={}", SERVICE_NAME, overview.getTotalPointCount(), overview.getSequenceCount());
            return result;
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] queryHistoryOverview failed: code={} desc={}", SERVICE_NAME, e.getStatus().getCode(), e.getStatus().getDescription());
            return Map.of("error", e.getStatus().getDescription());
        }
    }

    // ── window data ──────────────────────────────────────────────────

    public Map<String, Object> queryWindowData(HistoryDataQueryRequest request) {
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return Map.of("error", "core address not configured");
        }
        QueryWindowDataRequest.Builder b = QueryWindowDataRequest.newBuilder();
        b.addAllSequenceIds(resolveQuerySequenceIds(request));
        if (request != null && request.getStartTime() != null) {
            b.setStartTime(request.getStartTime().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli());
        }
        if (request != null && request.getEndTime() != null) {
            b.setEndTime(request.getEndTime().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli());
        }
        if (request != null) {
            b.setProjectId(nullToEmpty(request.getProjectId()));
        }
        LOG.info("[{}] -> queryWindowData at {}", SERVICE_NAME, address);
        ManagedChannel channel = channelRegistry.getChannel(address);
        try {
            QueryWindowDataResponse resp = TimeseriesCoreServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .queryWindowData(b.build());
            Map<String, Object> result = new java.util.LinkedHashMap<>();
            if (resp.hasData()) {
                WindowData wd = resp.getData();
                result.put("windowStartTime", wd.getWindowStartTime());
                result.put("windowEndTime", wd.getWindowEndTime());
                result.put("sequences", wd.getSequencesList().stream().map(seq -> {
                    Map<String, Object> sm = new java.util.LinkedHashMap<>();
                    sm.put("sequenceId", seq.getSequenceId());
                    sm.put("points", seq.getPointsList().stream().map(p -> {
                        Map<String, Object> pm = new java.util.LinkedHashMap<>();
                        pm.put("time", p.getTime());
                        if (p.hasValue()) {
                            TimeseriesValue v = p.getValue();
                            switch (v.getKindCase()) {
                                case DOUBLE_VALUE -> pm.put("value", v.getDoubleValue());
                                case INT64_VALUE -> pm.put("value", v.getInt64Value());
                                default -> pm.put("value", 0);
                            }
                        }
                        return pm;
                    }).collect(java.util.stream.Collectors.toList()));
                    return sm;
                }).collect(java.util.stream.Collectors.toList()));
            }
            LOG.info("[{}] <- queryWindowData seqs={}", SERVICE_NAME,
                    resp.hasData() ? resp.getData().getSequencesCount() : 0);
            return result;
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] queryWindowData failed: code={} desc={}", SERVICE_NAME, e.getStatus().getCode(), e.getStatus().getDescription());
            return Map.of("error", e.getStatus().getDescription());
        }
    }

    // ── alignment + statistics ────────────────────────────────────────

    /**
     * Align the Core hot window for the given sequences using a default
     * alignment config (every sequence with unspecified role, Core picks
     * default aggregation / gap-fill).
     */
    public AlignedWindowData alignWindowData(List<String> seqIds, String dependentSequenceId,
                                             List<String> relationIds,
                                             LocalDateTime startTime, LocalDateTime endTime) {
        return alignWindowData(null, seqIds, dependentSequenceId, relationIds, startTime, endTime);
    }

    public AlignedWindowData alignWindowData(String projectId, List<String> seqIds, String dependentSequenceId,
                                             List<String> relationIds,
                                             LocalDateTime startTime, LocalDateTime endTime) {
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            LOG.warn("[{}] alignWindowData skipped: address not configured", SERVICE_NAME);
            return null;
        }
        List<String> ids = seqIds != null ? seqIds : List.of();
        QueryWindowDataRequest.Builder wq = QueryWindowDataRequest.newBuilder()
                .addAllSequenceIds(ids)
                .setProjectId(nullToEmpty(projectId));
        if (startTime != null) {
            wq.setStartTime(startTime.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli());
        }
        if (endTime != null) {
            wq.setEndTime(endTime.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli());
        }
        AlignWindowDataRequest.Builder reqBuilder = AlignWindowDataRequest.newBuilder()
                .setWindowQuery(wq.build())
                .setConfig(buildAlignmentConfig(projectId, ids, dependentSequenceId))
                .setProjectId(nullToEmpty(projectId));
        if (relationIds != null && !relationIds.isEmpty()) {
            reqBuilder.addAllRelationIds(relationIds);
        }
        AlignWindowDataRequest req = reqBuilder.build();
        LOG.info("[{}] -> alignWindowData seqs={} dependent={} relations={} start={} end={} at {}",
                SERVICE_NAME, ids, dependentSequenceId, relationIds, startTime, endTime, address);
        ManagedChannel channel = channelRegistry.getChannel(address);
        try {
            AlignWindowDataResponse resp = TimeseriesCoreServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .alignWindowData(req);
            LOG.info("[{}] <- alignWindowData code={} msg={} samples={}",
                    SERVICE_NAME, resp.getOperation().getCode(), resp.getOperation().getMessage(),
                    resp.hasAlignedData() ? resp.getAlignedData().getSamplesCount() : 0);
            return resp.hasAlignedData() ? resp.getAlignedData() : null;
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] <- alignWindowData FAILED: code={} desc={}",
                    SERVICE_NAME, e.getStatus().getCode(), e.getStatus().getDescription());
            return null;
        }
    }

    /**
     * Compute basic statistics (per-sequence metrics + correlation vector)
     * over the given aligned data.
     */
    public ComputeStatisticsResponse computeBasicStatistics(AlignedWindowData alignedData,
                                                            List<String> seqIds, String dependentSequenceId,
                                                            String relationId) {
        return computeBasicStatistics(null, alignedData, seqIds, dependentSequenceId, relationId);
    }

    public ComputeStatisticsResponse computeBasicStatistics(String projectId, AlignedWindowData alignedData,
                                                            List<String> seqIds, String dependentSequenceId,
                                                            String relationId) {
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            LOG.warn("[{}] computeBasicStatistics skipped: address not configured", SERVICE_NAME);
            return null;
        }
        if (alignedData == null) {
            return null;
        }
        ComputeStatisticsRequest.Builder reqBuilder = ComputeStatisticsRequest.newBuilder()
                .setAlignedData(alignedData)
                .setAlignmentConfig(buildAlignmentConfig(projectId,
                        seqIds != null ? seqIds : List.of(), dependentSequenceId))
                .setProjectId(nullToEmpty(projectId));
        if (relationId != null && !relationId.isBlank()) {
            reqBuilder.setRelationId(relationId);
        }
        ComputeStatisticsRequest req = reqBuilder.build();
        ManagedChannel channel = channelRegistry.getChannel(address);
        try {
            ComputeStatisticsResponse resp = TimeseriesCoreServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .computeBasicStatistics(req);
            LOG.info("[{}] <- computeBasicStatistics code={} metrics={} hasCorrelation={}",
                    SERVICE_NAME, resp.getOperation().getCode(),
                    resp.getSequenceMetricsCount(), resp.hasCorrelationVector());
            return resp;
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] <- computeBasicStatistics FAILED: code={} desc={}",
                    SERVICE_NAME, e.getStatus().getCode(), e.getStatus().getDescription());
            return null;
        }
    }

    /**
     * Build the per-sequence alignment config. The dependent sequence is marked
     * {@code VARIABLE_ROLE_DEPENDENT} and the rest {@code VARIABLE_ROLE_INDEPENDENT};
     * Core uses this to know which variable the correlation vector is computed against.
     */
    private AlignmentConfig buildAlignmentConfig(List<String> seqIds, String dependentSequenceId) {
        return buildAlignmentConfig(null, seqIds, dependentSequenceId);
    }

    private AlignmentConfig buildAlignmentConfig(String projectId, List<String> seqIds, String dependentSequenceId) {
        String dependent = (dependentSequenceId == null || dependentSequenceId.isBlank())
                ? (seqIds != null && !seqIds.isEmpty() ? seqIds.get(0) : null)
                : dependentSequenceId;
        AlignmentConfig.Builder ac = AlignmentConfig.newBuilder();
        ac.setProjectId(nullToEmpty(projectId));
        for (String seqId : seqIds) {
            SequenceAlignmentConfig.Builder sc = SequenceAlignmentConfig.newBuilder()
                    .setSequenceId(seqId);
            sc.setRole(seqId != null && seqId.equals(dependent)
                    ? VariableRole.VARIABLE_ROLE_DEPENDENT
                    : VariableRole.VARIABLE_ROLE_INDEPENDENT);
            ac.addSequences(sc.build());
        }
        return ac.build();
    }

    /**
     * Resolve a service-level relation into the expanded Core relation ids
     * ({@code relationId_src_tgt}) registered by {@link #syncRelationConfig}.
     * Only same-device pairs whose target matches the dependent sequence and
     * whose source is among the requested sequences are returned.
     */
    public List<String> resolveExpandedRelationIds(TimeseriesRelation relation,
                                                   List<String> sequenceIds,
                                                   String dependentSequenceId) {
        if (relation == null || relation.getRelationId() == null
                || !"ENABLE".equalsIgnoreCase(relation.getEffectiveStatus())) {
            return List.of();
        }
        List<String> srcSeqIds = resolveToSequences(relation.getProjectId(), relation.getSourceSequences());
        List<String> tgtSeqIds = resolveToSequences(relation.getProjectId(),
                relation.getTargetSequenceId() != null
                        ? List.of(relation.getTargetSequenceId()) : List.of());
        if (dependentSequenceId != null && !dependentSequenceId.isBlank()) {
            tgtSeqIds = tgtSeqIds.stream()
                    .filter(dependentSequenceId::equals)
                    .collect(java.util.stream.Collectors.toList());
        }
        java.util.Set<String> requested = sequenceIds != null && !sequenceIds.isEmpty()
                ? new java.util.HashSet<>(sequenceIds)
                : java.util.Set.of();

        Map<String, List<String>> srcByDevice = groupByDeviceInstanceId(relation.getProjectId(), srcSeqIds);
        Map<String, List<String>> tgtByDevice = groupByDeviceInstanceId(relation.getProjectId(), tgtSeqIds);

        List<String> expanded = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : srcByDevice.entrySet()) {
            List<String> tgtInDevice = tgtByDevice.getOrDefault(entry.getKey(), List.of());
            for (String src : entry.getValue()) {
                for (String tgt : tgtInDevice) {
                    if (src.equals(tgt) || !requested.contains(src)) {
                        continue;
                    }
                    expanded.add(relation.getRelationId() + "_" + src + "_" + tgt);
                }
            }
        }
        return expanded;
    }

    // ── placeholder stubs ──────────────────────────────────────────────

    public Map<String, Object> queryStatistics(String sequenceId) {
        LOG.debug("[{}] queryStatistics seq={} - stub", SERVICE_NAME, sequenceId);
        return Map.of();
    }

    // ── internal helpers ───────────────────────────────────────────────


    private SyncResult callCoreSync(String address, CoreSyncCall callable, String operation) {
        ManagedChannel channel = channelRegistry.getChannel(address);
        try {
            SyncConfigResponse resp = callable.call(
                    TimeseriesCoreServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(3, TimeUnit.SECONDS));
            OperationResult op = resp.getOperation();
            boolean success = op.getCode() == OperationCode.OPERATION_CODE_OK
                    || op.getCode() == OperationCode.OPERATION_CODE_PARTIAL_SUCCESS;
            LOG.info("[{}] <- {} code={} success={} failed={} msg={}", SERVICE_NAME, operation,
                    op.getCode(), op.getSuccessCount(), op.getFailedCount(), op.getMessage());
            return SyncResult.of(success, op.getMessage());
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] <- {} FAILED: code={} desc={}", SERVICE_NAME, operation, e.getStatus().getCode(), e.getStatus().getDescription());
            return SyncResult.fail(e.getStatus().getDescription());
        }
    }

    @FunctionalInterface
    private interface CoreSyncCall {
        SyncConfigResponse call(TimeseriesCoreServiceGrpc.TimeseriesCoreServiceBlockingStub stub);
    }

    private SyncResult notConfigured(String operation) {
        LOG.info("[{}] {} skipped: core address not configured", SERVICE_NAME, operation);
        return SyncResult.fail("core address not configured");
    }

    private static String nullToEmpty(String v) { return v != null ? v : ""; }
    private static boolean isBlank(String s) { return s == null || s.isBlank(); }

    /**
     * Resolve sequence IDs from a query request, supporting both
     * {@code sequenceIds} (list) and {@code sequenceId} (single).
     */
    private static java.util.List<String> resolveQuerySequenceIds(HistoryDataQueryRequest request) {
        if (request == null) return java.util.List.of();
        if (request.getSequenceIds() != null && !request.getSequenceIds().isEmpty()) {
            return request.getSequenceIds();
        }
        if (request.getSequenceId() != null && !request.getSequenceId().isBlank()) {
            return java.util.List.of(request.getSequenceId());
        }
        return java.util.List.of();
    }
}
