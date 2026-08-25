package com.sfkg.timeseries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import com.sfkg.timeseries.config.GrpcClientProperties;
import com.sfkg.timeseries.dto.ConstraintSaveRequest;
import com.sfkg.timeseries.dto.ConstraintStatusUpdateRequest;
import com.sfkg.timeseries.dto.RelationSaveRequest;
import com.sfkg.timeseries.dto.RelationStatusUpdateRequest;
import com.sfkg.timeseries.entity.TimeseriesAnomalyTask;
import com.sfkg.timeseries.entity.TimeseriesCategory;
import com.sfkg.timeseries.entity.TimeseriesForecastTask;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.grpc.AnalysisStatus;
import com.sfkg.timeseries.grpc.AnalysisSyncAnomalyTaskRequest;
import com.sfkg.timeseries.grpc.AnalysisSyncForecastTaskRequest;
import com.sfkg.timeseries.grpc.ConstraintRule;
import com.sfkg.timeseries.grpc.OperationCode;
import com.sfkg.timeseries.grpc.OperationResult;
import com.sfkg.timeseries.grpc.RuntimeConstraintConfig;
import com.sfkg.timeseries.grpc.RuntimeRelationConfig;
import com.sfkg.timeseries.grpc.SyncConfigResponse;
import com.sfkg.timeseries.grpc.SyncConstraintsRequest;
import com.sfkg.timeseries.grpc.SyncRelationsRequest;
import com.sfkg.timeseries.grpc.TaskAck;
import com.sfkg.timeseries.grpc.TimeseriesAnalysisServiceGrpc;
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
 * 双端同步大规模验证：使用 in-process gRPC 假服务器捕获真实发出的报文，
 * 验证类别级约束/关系自动展开到实例级后，分别正确下发到
 * Core 端（SyncConstraints/SyncRelations）与 P 端（SyncAnomalyTask/SyncForecastTask 的语义上下文）。
 */
class SemanticDualSyncLargeScaleTests {

    private static final String P1 = "project-a";
    private static final String P2 = "project-b";

    private TimeseriesMemoryCache cache;
    private TimeseriesSemanticServiceImpl service;
    private TimeseriesTaskContextResolver contextResolver;

    private FakeCoreService coreFake;
    private FakeAnalysisService anomalyFake;
    private FakeAnalysisService forecastFake;
    private Server coreServer;
    private Server anomalyServer;
    private Server forecastServer;
    private TestChannelRegistry channelRegistry;

    @BeforeEach
    void setUp() throws IOException {
        cache = new TimeseriesMemoryCache();
        TimeseriesConstraintExpansionResolver expansionResolver =
                new TimeseriesConstraintExpansionResolver(cache);
        contextResolver = new TimeseriesTaskContextResolver(cache, expansionResolver);

        GrpcClientProperties properties = new GrpcClientProperties();
        String coreName = "core-" + UUID.randomUUID();
        String anomalyName = "anomaly-" + UUID.randomUUID();
        String forecastName = "forecast-" + UUID.randomUUID();
        properties.setCoreAddress(coreName);
        properties.setAnomalyAddress(anomalyName);
        properties.setForecastAddress(forecastName);

        coreFake = new FakeCoreService();
        coreServer = InProcessServerBuilder.forName(coreName).directExecutor()
                .addService(coreFake).build().start();
        anomalyFake = new FakeAnalysisService();
        anomalyServer = InProcessServerBuilder.forName(anomalyName).directExecutor()
                .addService(anomalyFake).build().start();
        forecastFake = new FakeAnalysisService();
        forecastServer = InProcessServerBuilder.forName(forecastName).directExecutor()
                .addService(forecastFake).build().start();

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
        if (anomalyServer != null) anomalyServer.shutdownNow();
        if (forecastServer != null) forecastServer.shutdownNow();
        if (channelRegistry != null) channelRegistry.shutdown();
    }

    // ── 1. 类别级约束：Core 收到实例级展开规则，P端任务语义上下文同步更新 ──

    @Test
    void categoryConstraintExpandsToInstanceRulesOnCoreAndRefreshesTasksOnPAnalysis() {
        buildWorld(P1, 3, List.of("OT", "HUFL"));

        // 新增：ENABLE，映射到类别 OT → 展开为 3 条实例规则
        service.saveConstraint(constraintRequest(P1, "c-ot", "OT bound", Map.of("x", "OT"), "x <= 100"));
        assertEquals(1, coreFake.constraintRequests.size());
        SyncConstraintsRequest first = coreFake.constraintRequests.get(0);
        assertEquals(P1, first.getProjectId());
        assertEquals(3, first.getItemsCount());
        assertEquals(Set.of("c-ot_" + seqId(P1, 1, "OT"),
                        "c-ot_" + seqId(P1, 2, "OT"), "c-ot_" + seqId(P1, 3, "OT")),
                first.getItemsList().stream()
                        .map(RuntimeConstraintConfig::getRule)
                        .map(ConstraintRule::getConstraintId)
                        .collect(Collectors.toSet()));
        assertRulePayload(first, "OT", true);

        // 任务就位：a-d1 目标 d1_OT，自动发现 c-ot（本设备规则）
        cache.putAnomalyTask(anomalyTask(P1, "a-d1", List.of(seqId(P1, 1, "OT")), List.of()));
        cache.putForecastTask(forecastTask(P1, "f-d1", List.of(seqId(P1, 1, "OT")), List.of()));

        // 禁用：Core 收到 enabled=false；P端任务重推且语义上下文清掉该约束
        service.updateConstraintStatus(statusRequest(P1, "c-ot", "DISABLE"));
        assertEquals(2, coreFake.constraintRequests.size());
        assertRulePayload(coreFake.constraintRequests.get(1), "OT", false);
        assertEquals(1, anomalyFake.anomalyRequests.size());
        assertEquals(1, forecastFake.forecastRequests.size());
        assertTrue(anomalyFake.anomalyRequests.get(0).getTask().getSemanticContext().getConstraintIdsList().isEmpty(),
                "禁用后 P端 任务上下文不应再携带该约束");
        assertTrue(forecastFake.forecastRequests.get(0).getTask().getSemanticContext().getConstraintIdsList().isEmpty());

        // 重新启用：P端任务上下文恢复本设备展开 ID（不含其他设备）
        service.updateConstraintStatus(statusRequest(P1, "c-ot", "ENABLE"));
        assertEquals(3, coreFake.constraintRequests.size());
        assertRulePayload(coreFake.constraintRequests.get(2), "OT", true);
        assertEquals(2, anomalyFake.anomalyRequests.size());
        List<String> ctxIds = anomalyFake.anomalyRequests.get(1).getTask().getSemanticContext().getConstraintIdsList();
        assertEquals(List.of("c-ot_" + seqId(P1, 1, "OT")), ctxIds);
        List<String> fCtxIds = forecastFake.forecastRequests.get(1).getTask().getSemanticContext().getConstraintIdsList();
        assertEquals(List.of("c-ot_" + seqId(P1, 1, "OT")), fCtxIds);

        // 修改映射到 HUFL：Core 收到新的 3 条 HUFL 规则；旧映射任务仍被重推且上下文被清空
        service.saveConstraint(constraintRequest(P1, "c-ot", "OT bound", Map.of("x", "HUFL"), "x <= 100"));
        assertEquals(4, coreFake.constraintRequests.size());
        assertRulePayload(coreFake.constraintRequests.get(3), "HUFL", true);
        assertEquals(3, anomalyFake.anomalyRequests.size());
        assertTrue(anomalyFake.anomalyRequests.get(2).getTask().getSemanticContext().getConstraintIdsList().isEmpty(),
                "映射改走后，旧目标任务的上下文不应再携带旧展开 ID");
    }

    // ── 2. 类别级关系：Core 收到同设备实例对，P端任务上下文包含关系 ──

    @Test
    void categoryRelationExpandsToSameDevicePairsOnCoreAndContextOnPAnalysis() {
        buildWorld(P1, 3, List.of("OT", "HUFL"));
        cache.putAnomalyTask(anomalyTask(P1, "a-d1", List.of(seqId(P1, 1, "OT")), List.of()));
        cache.putForecastTask(forecastTask(P1, "f-d2", List.of(seqId(P1, 2, "OT")), List.of()));

        service.saveRelation(relationRequest(P1, "r-hfl-ot", List.of("HUFL"), "OT", "CAUSE"));

        assertEquals(1, coreFake.relationRequests.size());
        SyncRelationsRequest request = coreFake.relationRequests.get(0);
        assertEquals(P1, request.getProjectId());
        assertEquals(3, request.getItemsCount(), "类别级关系应展开为同设备的 3 个实例对");
        for (RuntimeRelationConfig item : request.getItemsList()) {
            assertTrue(item.getEnabled());
            assertEquals(P1, item.getProjectId());
            String[] parts = item.getRelationId().split("_");
            // r-hfl-ot_<d>_HUFL_<d>_OT
            assertEquals("r-hfl-ot", parts[0]);
            assertTrue(item.getRelationId().matches("r-hfl-ot_project-a_d[123]_HUFL_project-a_d[123]_OT"));
            assertTrue(item.getRelationId().matches("r-hfl-ot_project-a_d(\\d)_HUFL_project-a_d\\1_OT"),
                    "关系只能展开为同设备 pair: " + item.getRelationId());
            assertEquals(1, item.getSourcesCount());
            assertEquals(item.getTargetSequenceId().replace("OT", "HUFL"), item.getSources(0).getSourceSequenceId());
            assertEquals("cause", item.getRelationType());
        }

        // P端：受影响任务被重推，语义上下文包含同设备关系 pair
        assertEquals(1, anomalyFake.anomalyRequests.size());
        assertEquals("a-d1", anomalyFake.anomalyRequests.get(0).getTask().getTaskId());
        assertTrue(anomalyFake.anomalyRequests.get(0).getTask().getSemanticContext().getRelationsList().stream()
                .anyMatch(r -> r.getRelationId().equals("r-hfl-ot_" + seqId(P1, 1, "HUFL") + "_" + seqId(P1, 1, "OT"))));

        assertEquals(1, forecastFake.forecastRequests.size());
        assertEquals("f-d2", forecastFake.forecastRequests.get(0).getTask().getTaskId());
        assertTrue(forecastFake.forecastRequests.get(0).getTask().getSemanticContext().getRelationsList().stream()
                .anyMatch(r -> r.getRelationId().equals("r-hfl-ot_" + seqId(P1, 2, "HUFL") + "_" + seqId(P1, 2, "OT"))));

        // 状态变更：Core 收到 enabled=false；任务再次重推
        RelationStatusUpdateRequest status = new RelationStatusUpdateRequest();
        status.setProjectId(P1);
        status.setRelationId("r-hfl-ot");
        status.setEffectiveStatus("DISABLE");
        service.updateRelationStatus(status);
        assertEquals(2, coreFake.relationRequests.size());
        assertFalse(coreFake.relationRequests.get(1).getItems(0).getEnabled());
        assertEquals(2, anomalyFake.anomalyRequests.size());
        assertEquals(2, forecastFake.forecastRequests.size());
    }

    // ── 3. 实例级约束/关系：1:1 直达两端，不产生多余展开 ──

    @Test
    void instanceLevelConstraintAndRelationMapOneToOneOnCore() {
        buildWorld(P1, 3, List.of("OT", "HUFL"));

        service.saveConstraint(constraintRequest(P1, "c-seq", "seq bound",
                Map.of("x", seqId(P1, 1, "OT")), "x <= 50"));
        assertEquals(1, coreFake.constraintRequests.size());
        SyncConstraintsRequest cReq = coreFake.constraintRequests.get(0);
        assertEquals(1, cReq.getItemsCount());
        assertEquals("c-seq_" + seqId(P1, 1, "OT"), cReq.getItems(0).getRule().getConstraintId());
        assertEquals(Map.of("x", seqId(P1, 1, "OT")), cReq.getItems(0).getRule().getVariableMappingMap());

        service.saveRelation(relationRequest(P1, "r-seq",
                List.of(seqId(P1, 1, "HUFL")), seqId(P1, 1, "OT"), "CAUSE"));
        assertEquals(1, coreFake.relationRequests.size());
        SyncRelationsRequest rReq = coreFake.relationRequests.get(0);
        assertEquals(1, rReq.getItemsCount());
        RuntimeRelationConfig item = rReq.getItems(0);
        assertEquals("r-seq_" + seqId(P1, 1, "HUFL") + "_" + seqId(P1, 1, "OT"), item.getRelationId());
        assertEquals(seqId(P1, 1, "OT"), item.getTargetSequenceId());
        assertEquals(1, item.getSourcesCount());
        assertEquals(seqId(P1, 1, "HUFL"), item.getSources(0).getSourceSequenceId());
    }

    // ── 4. 大规模：约束变更在两端的推送完整且不跨项目泄漏 ──

    @Test
    void largeScaleSyncReachesBothEndsWithoutCrossProjectLeakage() {
        buildWorld(P1, 3, List.of("OT", "HUFL"));
        buildWorld(P2, 2, List.of("OT"));

        int anomalyPerProject = 100;
        int forecastP1 = 50;
        for (int i = 0; i < anomalyPerProject; i++) {
            cache.putAnomalyTask(anomalyTask(P1, "p1-" + i,
                    List.of(seqId(P1, 1 + (i % 3), "OT")), List.of()));
            cache.putAnomalyTask(anomalyTask(P2, "p2-" + i,
                    List.of(seqId(P2, 1 + (i % 2), "OT")), List.of()));
        }
        for (int i = 0; i < forecastP1; i++) {
            cache.putForecastTask(forecastTask(P1, "f1-" + i,
                    List.of(seqId(P1, 1 + (i % 3), "OT")), List.of()));
        }

        service.saveConstraint(constraintRequest(P1, "c-bulk", "bulk", Map.of("x", "OT"), "x <= 100"));

        // Core 端：一次请求，3 条实例规则，仅 P1
        assertEquals(1, coreFake.constraintRequests.size());
        SyncConstraintsRequest coreReq = coreFake.constraintRequests.get(0);
        assertEquals(P1, coreReq.getProjectId());
        assertEquals(3, coreReq.getItemsCount());
        assertTrue(coreReq.getItemsList().stream()
                .allMatch(item -> item.getRule().getConstraintId().startsWith("c-bulk_project-a_")));

        // P端：P1 全部任务重推一次，P2 任务零推送
        List<AnalysisSyncAnomalyTaskRequest> anomalyReqs = anomalyFake.anomalyRequests;
        assertEquals(anomalyPerProject, anomalyReqs.size());
        assertEquals(anomalyPerProject,
                anomalyReqs.stream().map(r -> r.getTask().getTaskId()).collect(Collectors.toSet()).size(),
                "同一任务不应重复推送");
        assertTrue(anomalyReqs.stream().allMatch(r -> r.getTask().getProjectId().equals(P1)));
        assertTrue(anomalyReqs.stream().allMatch(r -> r.getTask().getTaskId().startsWith("p1-")));
        // 每个 P1 任务上下文都携带本设备展开约束，且不含其他设备
        for (AnalysisSyncAnomalyTaskRequest r : anomalyReqs) {
            List<String> ctxIds = r.getTask().getSemanticContext().getConstraintIdsList();
            assertEquals(1, ctxIds.size());
            assertTrue(ctxIds.get(0).matches("c-bulk_project-a_d[123]_OT"));
        }

        List<AnalysisSyncForecastTaskRequest> forecastReqs = forecastFake.forecastRequests;
        assertEquals(forecastP1, forecastReqs.size());
        assertTrue(forecastReqs.stream().allMatch(r -> r.getTask().getProjectId().equals(P1)));
        for (AnalysisSyncForecastTaskRequest r : forecastReqs) {
            List<String> ctxIds = r.getTask().getSemanticContext().getConstraintIdsList();
            assertEquals(1, ctxIds.size());
            assertTrue(ctxIds.get(0).matches("c-bulk_project-a_d[123]_OT"));
        }
    }

    // ── helpers ──────────────────────────────────────────────────────

    private static String seqId(String projectId, int device, String category) {
        return projectId + "_d" + device + "_" + category;
    }

    private void assertRulePayload(SyncConstraintsRequest request, String category, boolean enabled) {
        assertEquals(3, request.getItemsCount());
        for (RuntimeConstraintConfig item : request.getItemsList()) {
            assertEquals(enabled, item.getEnabled());
            assertEquals(P1, item.getProjectId());
            ConstraintRule rule = item.getRule();
            assertEquals(1, rule.getVariableMappingMap().size());
            String mappedSeq = rule.getVariableMappingMap().get("x");
            assertEquals(category, mappedSeq.substring(mappedSeq.lastIndexOf('_') + 1));
            assertEquals(rule.getConstraintId(), "c-ot_" + mappedSeq);
            assertEquals(1, rule.getTermsCount());
            assertEquals(1.0, rule.getTerms(0).getCoefficient());
            assertEquals("x", rule.getTerms(0).getVariable());
            assertEquals(0.0, rule.getLowerBound());
            assertEquals(100.0, rule.getUpperBound());
        }
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
                inst.setSequenceId(seqId(projectId, d, cat));
                inst.setInstanceName(seqId(projectId, d, cat));
                inst.setCategoryId(cat);
                inst.setDeviceInstanceId(deviceId);
                inst.setDataType("NUMERIC");
                cache.putInstanceConfig(inst);
            }
        }
    }

    private ConstraintSaveRequest constraintRequest(String projectId, String constraintId,
            String name, Map<String, String> mapping, String expression) {
        ConstraintSaveRequest request = new ConstraintSaveRequest();
        request.setProjectId(projectId);
        request.setConstraintId(constraintId);
        request.setConstraintName(name);
        request.setVariableMapping(mapping);
        request.setConstraintExpression(expression);
        request.setLowerBound(0.0);
        request.setUpperBound(100.0);
        request.setEffectiveStatus("ENABLE");
        request.setConfirmStatus("CONFIRMED");
        ConstraintSaveRequest.ConstraintTermDTO term = new ConstraintSaveRequest.ConstraintTermDTO();
        term.setVariable(mapping.keySet().stream().findFirst().orElse("x"));
        term.setCoefficient(1.0);
        term.setSampleOffset(0L);
        request.setTerms(List.of(term));
        return request;
    }

    private ConstraintStatusUpdateRequest statusRequest(String projectId, String constraintId, String effectiveStatus) {
        ConstraintStatusUpdateRequest request = new ConstraintStatusUpdateRequest();
        request.setProjectId(projectId);
        request.setConstraintId(constraintId);
        request.setEffectiveStatus(effectiveStatus);
        return request;
    }

    private RelationSaveRequest relationRequest(String projectId, String relationId,
            List<String> sources, String target, String type) {
        RelationSaveRequest request = new RelationSaveRequest();
        request.setProjectId(projectId);
        request.setRelationId(relationId);
        request.setRelationName(relationId);
        request.setSourceSequences(sources);
        request.setTargetSequenceId(target);
        request.setRelationType(type);
        request.setEffectiveStatus("ENABLE");
        request.setConfirmStatus("CONFIRMED");
        return request;
    }

    private TimeseriesAnomalyTask anomalyTask(String projectId, String taskId,
            List<String> sequenceIds, List<String> constraintIds) {
        TimeseriesAnomalyTask task = new TimeseriesAnomalyTask();
        task.setProjectId(projectId);
        task.setTaskId(taskId);
        task.setTaskName(taskId);
        task.setSequenceIds(sequenceIds);
        task.setConstraintIds(constraintIds);
        task.setStatus("ENABLE");
        return task;
    }

    private TimeseriesForecastTask forecastTask(String projectId, String taskId,
            List<String> forecastObjects, List<String> constraintIds) {
        TimeseriesForecastTask task = new TimeseriesForecastTask();
        task.setProjectId(projectId);
        task.setTaskId(taskId);
        task.setTaskName(taskId);
        task.setForecastObjects(forecastObjects);
        task.setConstraintIds(constraintIds);
        task.setStatus("ENABLE");
        return task;
    }

    // ── in-process fakes ─────────────────────────────────────────────

    private static class FakeCoreService extends TimeseriesCoreServiceGrpc.TimeseriesCoreServiceImplBase {
        final List<SyncConstraintsRequest> constraintRequests = new ArrayList<>();
        final List<SyncRelationsRequest> relationRequests = new ArrayList<>();

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

        @Override
        public void syncRelations(SyncRelationsRequest request, StreamObserver<SyncConfigResponse> responseObserver) {
            relationRequests.add(request);
            responseObserver.onNext(SyncConfigResponse.newBuilder()
                    .setOperation(OperationResult.newBuilder()
                            .setCode(OperationCode.OPERATION_CODE_OK)
                            .setSuccessCount(request.getItemsCount())
                            .setMessage("ok"))
                    .build());
            responseObserver.onCompleted();
        }
    }

    private static class FakeAnalysisService extends TimeseriesAnalysisServiceGrpc.TimeseriesAnalysisServiceImplBase {
        final List<AnalysisSyncAnomalyTaskRequest> anomalyRequests = new ArrayList<>();
        final List<AnalysisSyncForecastTaskRequest> forecastRequests = new ArrayList<>();

        @Override
        public void syncAnomalyTask(AnalysisSyncAnomalyTaskRequest request, StreamObserver<TaskAck> responseObserver) {
            anomalyRequests.add(request);
            responseObserver.onNext(TaskAck.newBuilder()
                    .setAccepted(true)
                    .setTaskId(request.getTask().getTaskId())
                    .setStatus(AnalysisStatus.ANALYSIS_STATUS_SUCCESS)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void syncForecastTask(AnalysisSyncForecastTaskRequest request, StreamObserver<TaskAck> responseObserver) {
            forecastRequests.add(request);
            responseObserver.onNext(TaskAck.newBuilder()
                    .setAccepted(true)
                    .setTaskId(request.getTask().getTaskId())
                    .setStatus(AnalysisStatus.ANALYSIS_STATUS_SUCCESS)
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
