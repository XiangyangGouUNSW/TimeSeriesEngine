package com.sfkg.timeseries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.client.AnomalyGrpcClient;
import com.sfkg.timeseries.client.ForecastGrpcClient;
import com.sfkg.timeseries.client.GrpcChannelRegistry;
import com.sfkg.timeseries.client.TimeseriesCoreGrpcClient;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.config.GrpcClientProperties;
import com.sfkg.timeseries.dto.ConstraintBatchSaveRequest;
import com.sfkg.timeseries.dto.ConstraintSaveRequest;
import com.sfkg.timeseries.entity.TimeseriesCategory;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.grpc.ConstraintAggregation;
import com.sfkg.timeseries.grpc.ConstraintRule;
import com.sfkg.timeseries.grpc.OperationCode;
import com.sfkg.timeseries.grpc.OperationResult;
import com.sfkg.timeseries.grpc.RuntimeConstraintConfig;
import com.sfkg.timeseries.grpc.SyncConfigResponse;
import com.sfkg.timeseries.grpc.SyncConstraintsRequest;
import com.sfkg.timeseries.grpc.TimeseriesCoreServiceGrpc;
import com.sfkg.timeseries.mapper.TimeseriesCategoryMapper;
import com.sfkg.timeseries.mapper.TimeseriesConstraintMapper;
import com.sfkg.timeseries.mapper.TimeseriesRelationMapper;
import com.sfkg.timeseries.service.TimeseriesConstraintExpansionResolver;
import com.sfkg.timeseries.service.TimeseriesTaskContextResolver;
import com.sfkg.timeseries.service.impl.TimeseriesSemanticServiceImpl;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;

/**
 * OR 组批量创建大规模验证：
 * <ul>
 *   <li>批量接口一次创建大量约束，且每批只发一次 Core syncConstraints（组原子送达）；</li>
 *   <li>类别级展开时 or_group_id 追加序列后缀：同设备成员共享后缀组 ID（组内 OR），
 *       不同设备组 ID 不同（组间独立），不再丢弃组 ID；</li>
 *   <li>聚合方式（AVERAGE/MAXIMUM/MINIMUM/SAMPLE）透传为 proto 枚举；</li>
 *   <li>普通单条约束的 or_group_id 为空；</li>
 *   <li>批量校验失败时整批不落库、不发出任何 Core 请求。</li>
 * </ul>
 */
class ConstraintOrGroupLargeScaleTests {

    private static final String P1 = "project-ett";

    private TimeseriesMemoryCache cache;
    private TimeseriesSemanticServiceImpl service;
    private FakeCoreService coreFake;
    private Server coreServer;
    private TestChannelRegistry channelRegistry;

    @BeforeEach
    void setUp() throws IOException {
        cache = new TimeseriesMemoryCache();
        TimeseriesConstraintExpansionResolver expansionResolver =
                new TimeseriesConstraintExpansionResolver(cache);
        TimeseriesTaskContextResolver contextResolver =
                new TimeseriesTaskContextResolver(cache, expansionResolver);

        GrpcClientProperties properties = new GrpcClientProperties();
        String coreName = "core-" + UUID.randomUUID();
        properties.setCoreAddress(coreName);

        coreFake = new FakeCoreService();
        coreServer = InProcessServerBuilder.forName(coreName).directExecutor()
                .addService(coreFake).build().start();
        channelRegistry = new TestChannelRegistry();

        TimeseriesCoreGrpcClient coreClient = new TimeseriesCoreGrpcClient(
                properties, new ObjectMapper(), cache, channelRegistry, expansionResolver);
        AnomalyGrpcClient anomalyClient = new AnomalyGrpcClient(properties, contextResolver, channelRegistry);
        ForecastGrpcClient forecastClient = new ForecastGrpcClient(properties, contextResolver, channelRegistry);

        service = new TimeseriesSemanticServiceImpl(
                mock(TimeseriesCategoryMapper.class),
                mock(TimeseriesConstraintMapper.class),
                mock(TimeseriesRelationMapper.class),
                cache, mock(TimeseriesCacheManager.class),
                coreClient, anomalyClient, forecastClient, contextResolver);
    }

    @AfterEach
    void tearDown() {
        if (coreServer != null) coreServer.shutdownNow();
        if (channelRegistry != null) channelRegistry.shutdown();
    }

    // ── 1. 大规模批量：20 组 × 8 成员，类别级展开到 3 设备 ──

    @Test
    void largeScaleBatchCreatesAllMembersAndSendsOneAtomicCoreRequestPerGroup() {
        int deviceCount = 3;
        buildWorld(P1, deviceCount, List.of("OT"));

        int groupCount = 20;
        int membersPerGroup = 8;

        for (int g = 0; g < groupCount; g++) {
            ConstraintBatchSaveRequest batch = new ConstraintBatchSaveRequest();
            batch.setProjectId(P1);
            batch.setOrGroupId("band-" + g);
            List<ConstraintSaveRequest> members = new ArrayList<>();
            for (int m = 0; m < membersPerGroup; m++) {
                members.add(memberRequest(
                        "band-" + g + "-m" + m,
                        m % 2 == 0 ? "x < 10" : "x > 40",
                        m % 2 == 0 ? null : 40.0,
                        m % 2 == 0 ? 10.0 : null,
                        m % 3 == 0 ? "AVERAGE" : "SAMPLE"));
            }
            batch.setConstraints(members);
            List<String> ids = service.createConstraintBatch(batch);
            assertEquals(membersPerGroup, ids.size(), "每组成员应全部创建");
        }

        // Core 端：每组恰好一次 syncConstraints（组原子送达）
        assertEquals(groupCount, coreFake.constraintRequests.size(),
                "每个 OR 组应只发一次 Core syncConstraints");

        for (int g = 0; g < groupCount; g++) {
            SyncConstraintsRequest req = coreFake.constraintRequests.get(g);
            assertEquals(P1, req.getProjectId());
            assertEquals(membersPerGroup * deviceCount, req.getItemsCount(),
                    "组成员 × 设备数 = 展开后的规则数");
            List<RuntimeConstraintConfig> items = req.getItemsList();
            for (RuntimeConstraintConfig item : items) {
                assertTrue(item.getEnabled());
                ConstraintRule rule = item.getRule();
                // constraint_id 带序列后缀
                assertTrue(rule.getConstraintId().matches("band-" + g + "-m\\d_" + seqId(P1, "\\d", "OT")),
                        "constraint_id 应带序列后缀: " + rule.getConstraintId());
                // or_group_id 带序列后缀（不丢弃）
                assertTrue(rule.getOrGroupId().matches("band-" + g + "_" + seqId(P1, "\\d", "OT")),
                        "or_group_id 应带序列后缀: " + rule.getOrGroupId());
            }

            // 同设备规则共享同一后缀组 ID，不同设备组 ID 不同
            Set<String> groupIds = items.stream()
                    .map(item -> item.getRule().getOrGroupId())
                    .collect(Collectors.toSet());
            assertEquals(deviceCount, groupIds.size(),
                    "展开后应按设备拆分为 " + deviceCount + " 个独立 OR 子句");
            for (String deviceGroupId : groupIds) {
                long count = items.stream()
                        .filter(item -> item.getRule().getOrGroupId().equals(deviceGroupId))
                        .count();
                assertEquals(membersPerGroup, count, "同设备子句应包含全部组成员");
            }
        }
    }

    // ── 2. 同组「同序列映射」成员 OR 成同一个 clause，跨设备不合并 ──

    @Test
    void sameSequenceMembersShareGroupPerDeviceWithoutCrossDeviceMerge() {
        buildWorld(P1, 3, List.of("OT"));

        ConstraintBatchSaveRequest batch = new ConstraintBatchSaveRequest();
        batch.setProjectId(P1);
        batch.setOrGroupId("ot-band");
        batch.setConstraints(List.of(
                memberRequest("ot-below-10", "x < 10", null, 10.0, "SAMPLE"),
                memberRequest("ot-above-40", "x > 40", 40.0, null, "SAMPLE")));
        service.createConstraintBatch(batch);

        assertEquals(1, coreFake.constraintRequests.size());
        SyncConstraintsRequest req = coreFake.constraintRequests.get(0);
        assertEquals(6, req.getItemsCount());

        Map<String, List<ConstraintRule>> clauses = new LinkedHashMap<>();
        for (RuntimeConstraintConfig item : req.getItemsList()) {
            clauses.computeIfAbsent(item.getRule().getOrGroupId(), k -> new ArrayList<>())
                    .add(item.getRule());
        }
        assertEquals(3, clauses.size(), "2 成员 × 3 设备 = 3 个独立 OR 子句");
        for (List<ConstraintRule> members : clauses.values()) {
            assertEquals(2, members.size(), "每个子句应包含 2 个组成员");
            Set<String> ids = members.stream().map(ConstraintRule::getConstraintId).collect(Collectors.toSet());
            Set<String> groups = members.stream().map(ConstraintRule::getOrGroupId).collect(Collectors.toSet());
            assertEquals(1, groups.size());
            assertEquals(2, ids.size());
            String seq = groups.iterator().next().substring("ot-band_".length());
            for (ConstraintRule rule : members) {
                assertEquals(seq, rule.getVariableMappingMap().get("x"),
                        "同一子句的成员必须映射到同一序列");
            }
        }
    }

    // ── 3. 聚合方式透传 ──

    @Test
    void aggregationValuesPassThroughToCoreAsProtoEnums() {
        buildWorld(P1, 1, List.of("OT"));

        ConstraintBatchSaveRequest batch = new ConstraintBatchSaveRequest();
        batch.setProjectId(P1);
        batch.setOrGroupId("agg-group");
        List<ConstraintSaveRequest> members = new ArrayList<>();
        members.add(memberRequest("agg-sample", "x < 5", null, 5.0, "SAMPLE"));
        members.add(memberRequest("agg-avg", "x < 6", null, 6.0, "AVERAGE"));
        members.add(memberRequest("agg-max", "x < 7", null, 7.0, "MAXIMUM"));
        members.add(memberRequest("agg-min", "x < 8", null, 8.0, "MINIMUM"));
        members.add(memberRequest("agg-blank", "x < 9", null, 9.0, null));
        batch.setConstraints(members);
        service.createConstraintBatch(batch);

        assertEquals(1, coreFake.constraintRequests.size());
        Map<String, ConstraintAggregation> byId = new HashMap<>();
        for (RuntimeConstraintConfig item : coreFake.constraintRequests.get(0).getItemsList()) {
            byId.put(item.getRule().getConstraintId(),
                    item.getRule().getTerms(0).getAggregation());
        }
        assertAggregation(byId, "agg-sample_", ConstraintAggregation.CONSTRAINT_AGGREGATION_SAMPLE);
        assertAggregation(byId, "agg-avg_", ConstraintAggregation.CONSTRAINT_AGGREGATION_AVERAGE);
        assertAggregation(byId, "agg-max_", ConstraintAggregation.CONSTRAINT_AGGREGATION_MAXIMUM);
        assertAggregation(byId, "agg-min_", ConstraintAggregation.CONSTRAINT_AGGREGATION_MINIMUM);
        // 空聚合回退 SAMPLE
        assertAggregation(byId, "agg-blank_", ConstraintAggregation.CONSTRAINT_AGGREGATION_SAMPLE);
    }

    // ── 4. 普通单条约束：or_group_id 为空 ──

    @Test
    void regularSingleConstraintCarriesEmptyOrGroupId() {
        buildWorld(P1, 1, List.of("OT"));

        ConstraintSaveRequest single = memberRequest("plain-c", "x < 20", null, 20.0, "SAMPLE");
        service.saveConstraint(single);

        assertEquals(1, coreFake.constraintRequests.size());
        ConstraintRule rule = coreFake.constraintRequests.get(0).getItems(0).getRule();
        assertTrue(rule.getOrGroupId().isEmpty(), "普通约束的 or_group_id 应为空");
    }

    // ── 5. 批量校验失败：整批不落库、不发 Core 请求 ──

    @Test
    void batchValidationFailuresRejectWholeBatchWithoutCoreRequests() {
        buildWorld(P1, 1, List.of("OT"));

        // 空组 ID
        ConstraintBatchSaveRequest emptyGroup = new ConstraintBatchSaveRequest();
        emptyGroup.setProjectId(P1);
        emptyGroup.setOrGroupId("");
        emptyGroup.setConstraints(List.of(memberRequest("m1", "x < 5", null, 5.0, "SAMPLE")));
        assertThrows(BusinessException.class, () -> service.createConstraintBatch(emptyGroup));

        // 批内重复约束 ID
        ConstraintBatchSaveRequest dup = new ConstraintBatchSaveRequest();
        dup.setProjectId(P1);
        dup.setOrGroupId("g-dup");
        dup.setConstraints(List.of(
                memberRequest("dup-id", "x < 5", null, 5.0, "SAMPLE"),
                memberRequest("dup-id", "x > 6", 6.0, null, "SAMPLE")));
        assertThrows(BusinessException.class, () -> service.createConstraintBatch(dup));

        // 与存量约束 ID 冲突
        service.saveConstraint(memberRequest("existing-c", "x < 20", null, 20.0, "SAMPLE"));
        int coreRequestsAfterSingle = coreFake.constraintRequests.size();
        ConstraintBatchSaveRequest conflict = new ConstraintBatchSaveRequest();
        conflict.setProjectId(P1);
        conflict.setOrGroupId("g-conflict");
        conflict.setConstraints(List.of(
                memberRequest("existing-c", "x < 5", null, 5.0, "SAMPLE"),
                memberRequest("new-c", "x > 6", 6.0, null, "SAMPLE")));
        assertThrows(BusinessException.class, () -> service.createConstraintBatch(conflict));

        // 以上失败批次均未产生新的 Core 请求
        assertEquals(coreRequestsAfterSingle, coreFake.constraintRequests.size(),
                "校验失败的批次不应发出 Core syncConstraints");
        // 冲突批次中未被冲突的成员也不应落库
        assertFalse(cache.getConstraint(P1, "new-c").isPresent(),
                "校验失败时批内其他成员也不应写入");
    }

    // ── helpers ──────────────────────────────────────────────────────

    private static String seqId(String projectId, String device, String category) {
        return projectId + "_d" + device + "_" + category;
    }

    private void buildWorld(String projectId, int deviceCount, List<String> categories) {
        for (String cat : categories) {
            TimeseriesCategory c = new TimeseriesCategory();
            c.setProjectId(projectId);
            c.setCategoryId(cat);
            c.setCategoryName(cat);
            c.setConfirmStatus("CONFIRMED");
            cache.putCategory(c);
        }
        for (int d = 1; d <= deviceCount; d++) {
            String deviceId = projectId + "-device-" + d;
            for (String cat : categories) {
                TimeseriesInstanceConfig inst = new TimeseriesInstanceConfig();
                inst.setProjectId(projectId);
                inst.setSequenceId(seqId(projectId, String.valueOf(d), cat));
                inst.setInstanceName(seqId(projectId, String.valueOf(d), cat));
                inst.setCategoryId(cat);
                inst.setDeviceInstanceId(deviceId);
                inst.setDataType("NUMERIC");
                cache.putInstanceConfig(inst);
            }
        }
    }

    private ConstraintSaveRequest memberRequest(String constraintId, String expression,
            Double lowerBound, Double upperBound, String aggregation) {
        ConstraintSaveRequest request = new ConstraintSaveRequest();
        request.setProjectId(P1);
        request.setConstraintId(constraintId);
        request.setConstraintName(constraintId);
        request.setVariableMapping(Map.of("x", "OT"));
        request.setConstraintExpression(expression);
        request.setLowerBound(lowerBound);
        request.setUpperBound(upperBound);
        request.setEffectiveStatus("ENABLE");
        request.setConfirmStatus("CONFIRMED");
        ConstraintSaveRequest.ConstraintTermDTO term = new ConstraintSaveRequest.ConstraintTermDTO();
        term.setVariable("x");
        term.setCoefficient(1.0);
        term.setSampleOffset(0L);
        term.setAggregation(aggregation);
        request.setTerms(List.of(term));
        return request;
    }

    private void assertAggregation(Map<String, ConstraintAggregation> byId,
            String idPrefix, ConstraintAggregation expected) {
        long matched = byId.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(idPrefix))
                .count();
        assertEquals(1, matched, "应恰好一条规则的 ID 以 " + idPrefix + " 开头");
        byId.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(idPrefix))
                .forEach(entry -> assertEquals(expected, entry.getValue(),
                        "聚合方式透传错误: " + entry.getKey()));
    }

    // ── in-process fakes ─────────────────────────────────────────────

    private static class FakeCoreService extends TimeseriesCoreServiceGrpc.TimeseriesCoreServiceImplBase {
        final List<SyncConstraintsRequest> constraintRequests = new ArrayList<>();

        @Override
        public void syncConstraints(SyncConstraintsRequest request, StreamObserver<SyncConfigResponse> responseObserver) {
            constraintRequests.add(request);
            responseObserver.onNext(SyncConfigResponse.newBuilder()
                    .setOperation(OperationResult.newBuilder()
                            .setCode(OperationCode.OPERATION_CODE_OK)
                            .setSuccessCount(request.getItemsCount())
                            .setMessage("ok"))
                    .build());
            responseObserver.onCompleted();
        }
    }

    /** 将逻辑地址直接映射为同名的 in-process 服务名。 */
    private static class TestChannelRegistry extends GrpcChannelRegistry {
        private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

        @Override
        public ManagedChannel getChannel(String address) {
            return channels.computeIfAbsent(address,
                    name -> InProcessChannelBuilder.forName(name).directExecutor().build());
        }

        @Override
        public void shutdown() {
            channels.values().forEach(ManagedChannel::shutdownNow);
            channels.clear();
        }
    }
}
