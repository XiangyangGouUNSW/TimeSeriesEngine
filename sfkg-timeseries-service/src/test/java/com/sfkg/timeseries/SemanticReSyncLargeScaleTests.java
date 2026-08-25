package com.sfkg.timeseries;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.client.AnomalyGrpcClient;
import com.sfkg.timeseries.client.ForecastGrpcClient;
import com.sfkg.timeseries.client.TimeseriesCoreGrpcClient;
import com.sfkg.timeseries.dto.ConstraintSaveRequest;
import com.sfkg.timeseries.dto.ConstraintStatusUpdateRequest;
import com.sfkg.timeseries.dto.RelationSaveRequest;
import com.sfkg.timeseries.dto.RelationStatusUpdateRequest;
import com.sfkg.timeseries.entity.TimeseriesAnomalyTask;
import com.sfkg.timeseries.entity.TimeseriesCategory;
import com.sfkg.timeseries.entity.TimeseriesForecastTask;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.grpc.SemanticContext;
import com.sfkg.timeseries.mapper.TimeseriesCategoryMapper;
import com.sfkg.timeseries.mapper.TimeseriesConstraintMapper;
import com.sfkg.timeseries.mapper.TimeseriesRelationMapper;
import com.sfkg.timeseries.service.TimeseriesConstraintExpansionResolver;
import com.sfkg.timeseries.service.TimeseriesTaskContextResolver;
import com.sfkg.timeseries.service.impl.TimeseriesSemanticServiceImpl;

/**
 * 大规模语义链路回归：覆盖约束/关系的存储入口、自动发现（sequenceId→categoryId→constraint、
 * category→sequence 展开）、以及修改约束/关系后向 P端（Anomaly/Forecast）的任务重推逻辑。
 * gRPC 客户端为 mock，缓存/解析器/服务实现均为真实对象。
 */
class SemanticReSyncLargeScaleTests {

    private static final String P1 = "project-a";
    private static final String P2 = "project-b";

    private TimeseriesMemoryCache cache;
    private TimeseriesTaskContextResolver contextResolver;
    private TimeseriesSemanticServiceImpl service;
    private AnomalyGrpcClient anomalyClient;
    private ForecastGrpcClient forecastClient;
    private TimeseriesCoreGrpcClient coreClient;

    @BeforeEach
    void setUp() {
        cache = new TimeseriesMemoryCache();
        anomalyClient = mock(AnomalyGrpcClient.class);
        forecastClient = mock(ForecastGrpcClient.class);
        coreClient = mock(TimeseriesCoreGrpcClient.class);
        TimeseriesCacheManager cacheManager = mock(TimeseriesCacheManager.class);
        TimeseriesConstraintExpansionResolver expansionResolver =
                new TimeseriesConstraintExpansionResolver(cache);
        contextResolver = new TimeseriesTaskContextResolver(cache, expansionResolver);
        service = new TimeseriesSemanticServiceImpl(
                mock(TimeseriesCategoryMapper.class),
                mock(TimeseriesConstraintMapper.class),
                mock(TimeseriesRelationMapper.class),
                cache, cacheManager, coreClient, anomalyClient, forecastClient, contextResolver);
    }

    // ── 1. 新增约束 → 显式引用与自动发现的任务都重推 P端 ──────────────

    @Test
    void savingNewConstraintReSyncsExplicitAndAutoDiscoveredTasks() {
        buildWorld(P1, 3, List.of("OT", "HUFL"));
        buildWorld(P2, 2, List.of("OT"));

        cache.putAnomalyTask(anomalyTask(P1, "a-explicit",
                List.of(seqId(P1, 2, "HUFL")), List.of("c-ot-max")));
        cache.putAnomalyTask(anomalyTask(P1, "a-auto", List.of(seqId(P1, 1, "OT")), List.of()));
        cache.putAnomalyTask(anomalyTask(P1, "a-hufl", List.of(seqId(P1, 1, "HUFL")), List.of()));
        cache.putAnomalyTask(anomalyTask(P2, "a-p2", List.of(seqId(P2, 1, "OT")), List.of()));
        cache.putForecastTask(forecastTask(P1, "f-auto", List.of(seqId(P1, 1, "OT")), List.of()));
        cache.putForecastTask(forecastTask(P2, "f-p2", List.of(seqId(P2, 1, "OT")), List.of()));

        clearInvocations(anomalyClient, forecastClient, coreClient);
        service.saveConstraint(constraintRequest(P1, "c-ot-max", "OT max", Map.of("x", "OT"), "x <= 100"));

        ArgumentCaptor<TimeseriesAnomalyTask> anomalyCaptor =
                ArgumentCaptor.forClass(TimeseriesAnomalyTask.class);
        verify(anomalyClient, times(2)).syncAnomalyTask(anomalyCaptor.capture());
        assertEquals(Set.of("a-explicit", "a-auto"),
                anomalyCaptor.getAllValues().stream()
                        .map(TimeseriesAnomalyTask::getTaskId).collect(Collectors.toSet()));

        ArgumentCaptor<TimeseriesForecastTask> forecastCaptor =
                ArgumentCaptor.forClass(TimeseriesForecastTask.class);
        verify(forecastClient, times(1)).syncForecastTask(forecastCaptor.capture());
        assertEquals("f-auto", forecastCaptor.getValue().getTaskId());

        // 约束本体已同步到 Core
        verify(coreClient, times(1)).syncConstraintConfig(any());
    }

    // ── 2. 禁用约束 → 自动发现的任务也要重推（清除 P端旧快照） ──────────

    @Test
    void disablingConstraintStillReSyncsAutoDiscoveredTasks() {
        buildWorld(P1, 3, List.of("OT", "HUFL"));
        service.saveConstraint(constraintRequest(P1, "c-ot-max", "OT max", Map.of("x", "OT"), "x <= 100"));

        cache.putAnomalyTask(anomalyTask(P1, "a-d1", List.of(seqId(P1, 1, "OT")), List.of()));
        cache.putAnomalyTask(anomalyTask(P1, "a-d2", List.of(seqId(P1, 2, "OT")), List.of()));
        cache.putAnomalyTask(anomalyTask(P1, "a-hufl", List.of(seqId(P1, 1, "HUFL")), List.of()));

        clearInvocations(anomalyClient, forecastClient);
        ConstraintStatusUpdateRequest request = new ConstraintStatusUpdateRequest();
        request.setProjectId(P1);
        request.setConstraintId("c-ot-max");
        request.setEffectiveStatus("DISABLE");
        service.updateConstraintStatus(request);

        ArgumentCaptor<TimeseriesAnomalyTask> captor = ArgumentCaptor.forClass(TimeseriesAnomalyTask.class);
        verify(anomalyClient, times(2)).syncAnomalyTask(captor.capture());
        assertEquals(Set.of("a-d1", "a-d2"),
                captor.getAllValues().stream().map(TimeseriesAnomalyTask::getTaskId).collect(Collectors.toSet()));
        verify(forecastClient, never()).syncForecastTask(any());
    }

    // ── 3. 修改 variableMapping → 新旧映射命中的任务都要重推 ────────────

    @Test
    void variableMappingChangeReSyncsTasksOfOldAndNewMapping() {
        buildWorld(P1, 3, List.of("OT", "HUFL"));
        service.saveConstraint(constraintRequest(P1, "c-move", "move",
                Map.of("x", seqId(P1, 1, "OT")), "x <= 50"));

        cache.putAnomalyTask(anomalyTask(P1, "a-old", List.of(seqId(P1, 1, "OT")), List.of()));
        cache.putAnomalyTask(anomalyTask(P1, "a-new", List.of(seqId(P1, 2, "OT")), List.of()));
        cache.putAnomalyTask(anomalyTask(P1, "a-far", List.of(seqId(P1, 3, "OT")), List.of()));

        clearInvocations(anomalyClient);
        service.saveConstraint(constraintRequest(P1, "c-move", "move",
                Map.of("x", seqId(P1, 2, "OT")), "x <= 50"));

        ArgumentCaptor<TimeseriesAnomalyTask> captor = ArgumentCaptor.forClass(TimeseriesAnomalyTask.class);
        verify(anomalyClient, times(2)).syncAnomalyTask(captor.capture());
        assertEquals(Set.of("a-old", "a-new"),
                captor.getAllValues().stream().map(TimeseriesAnomalyTask::getTaskId).collect(Collectors.toSet()));
    }

    // ── 4. 新增/改状态关系 → 序列级与类别级命中的任务都重推 P端 ──────────

    @Test
    void relationSaveAndStatusUpdateReSyncAffectedTasks() {
        buildWorld(P1, 3, List.of("OT", "HUFL"));
        buildWorld(P2, 2, List.of("OT"));

        cache.putAnomalyTask(anomalyTask(P1, "a-ot", List.of(seqId(P1, 1, "OT")), List.of()));
        cache.putAnomalyTask(anomalyTask(P1, "a-hufl", List.of(seqId(P1, 1, "HUFL")), List.of()));
        cache.putAnomalyTask(anomalyTask(P2, "a-p2", List.of(seqId(P2, 1, "OT")), List.of()));
        cache.putForecastTask(forecastTask(P1, "f-ot", List.of(seqId(P1, 1, "OT")), List.of()));
        cache.putForecastTask(forecastTask(P2, "f-p2", List.of(seqId(P2, 1, "OT")), List.of()));

        // 序列级关系：受影响序列 = {d1_HUFL, d1_OT}
        clearInvocations(anomalyClient, forecastClient);
        service.saveRelation(relationRequest(P1, "r-seq",
                List.of(seqId(P1, 1, "HUFL")), seqId(P1, 1, "OT"), "CAUSE"));

        ArgumentCaptor<TimeseriesAnomalyTask> anomalyCaptor =
                ArgumentCaptor.forClass(TimeseriesAnomalyTask.class);
        verify(anomalyClient, times(2)).syncAnomalyTask(anomalyCaptor.capture());
        assertEquals(Set.of("a-ot", "a-hufl"),
                anomalyCaptor.getAllValues().stream().map(TimeseriesAnomalyTask::getTaskId).collect(Collectors.toSet()));

        ArgumentCaptor<TimeseriesForecastTask> forecastCaptor =
                ArgumentCaptor.forClass(TimeseriesForecastTask.class);
        verify(forecastClient, times(1)).syncForecastTask(forecastCaptor.capture());
        assertEquals("f-ot", forecastCaptor.getValue().getTaskId());

        // 状态变更路径同样重推
        clearInvocations(anomalyClient, forecastClient);
        RelationStatusUpdateRequest statusRequest = new RelationStatusUpdateRequest();
        statusRequest.setProjectId(P1);
        statusRequest.setRelationId("r-seq");
        statusRequest.setEffectiveStatus("DISABLE");
        service.updateRelationStatus(statusRequest);
        verify(anomalyClient, times(2)).syncAnomalyTask(any());
        verify(forecastClient, times(1)).syncForecastTask(any());

        // 类别级关系：受影响序列 = 全部 HUFL + 全部 OT 序列（跨设备展开）
        clearInvocations(anomalyClient, forecastClient);
        service.saveRelation(relationRequest(P1, "r-cat", List.of("HUFL"), "OT", "CAUSE"));
        ArgumentCaptor<TimeseriesAnomalyTask> catCaptor = ArgumentCaptor.forClass(TimeseriesAnomalyTask.class);
        verify(anomalyClient, times(2)).syncAnomalyTask(catCaptor.capture());
        assertEquals(Set.of("a-ot", "a-hufl"),
                catCaptor.getAllValues().stream().map(TimeseriesAnomalyTask::getTaskId).collect(Collectors.toSet()));
        verify(forecastClient, times(1)).syncForecastTask(any());
    }

    // ── 5. 上下文解析：类别展开、跨设备约束 ID、关系展开 ─────────────────

    @Test
    void contextResolutionExpandsCategoriesConstraintsAndRelations() {
        buildWorld(P1, 3, List.of("OT", "HUFL", "MULL"));
        service.saveConstraint(constraintRequest(P1, "c-ot", "OT bound", Map.of("x", "OT"), "x <= 100"));
        service.saveConstraint(constraintRequest(P1, "c-hufl", "HUFL bound", Map.of("y", "HUFL"), "y <= 200"));
        service.saveRelation(relationRequest(P1, "r-hfl-ot", List.of("HUFL"), "OT", "CAUSE"));

        for (int d = 1; d <= 3; d++) {
            final int device = d;
            TimeseriesAnomalyTask otTask = anomalyTask(P1, "a-" + device, List.of(seqId(P1, device, "OT")), List.of());
            cache.putAnomalyTask(otTask);
            SemanticContext ctx = contextResolver.resolveAnomalyContext(otTask);

            // 类别级约束按设备展开，只包含本设备规则
            assertTrue(ctx.getConstraintIdsList().contains("c-ot_" + seqId(P1, device, "OT")), "device " + device);
            for (int other = 1; other <= 3; other++) {
                if (other != device) {
                    assertFalse(ctx.getConstraintIdsList().contains("c-ot_" + seqId(P1, other, "OT")),
                            "device " + device + " 不应包含 device " + other + " 的约束");
                }
            }
            // 类别级关系展开为具体 pair，同设备 pair 存在
            assertTrue(ctx.getRelationsList().stream()
                    .anyMatch(r -> r.getRelationId().equals(
                            "r-hfl-ot_" + seqId(P1, device, "HUFL") + "_" + seqId(P1, device, "OT"))),
                    "device " + device);
            // 关系来源序列以 FEATURE 元数据进入上下文
            assertTrue(ctx.getSequencesList().stream()
                    .anyMatch(s -> seqId(P1, device, "HUFL").equals(s.getSequenceId())), "device " + device);

            // HUFL 任务自动发现 c-hufl
            TimeseriesAnomalyTask huflTask = anomalyTask(P1, "a-hufl-" + device,
                    List.of(seqId(P1, device, "HUFL")), List.of());
            cache.putAnomalyTask(huflTask);
            SemanticContext hctx = contextResolver.resolveAnomalyContext(huflTask);
            assertTrue(hctx.getConstraintIdsList().contains("c-hufl_" + seqId(P1, device, "HUFL")), "device " + device);

            // Forecast 同样自动发现目标序列的约束
            TimeseriesForecastTask fTask = forecastTask(P1, "f-" + device, List.of(seqId(P1, device, "OT")), List.of());
            cache.putForecastTask(fTask);
            SemanticContext fctx = contextResolver.resolveForecastContext(fTask);
            assertTrue(fctx.getConstraintIdsList().contains("c-ot_" + seqId(P1, device, "OT")), "device " + device);
            assertTrue(fctx.getRelationsList().stream()
                    .anyMatch(r -> r.getRelationId().equals(
                            "r-hfl-ot_" + seqId(P1, device, "HUFL") + "_" + seqId(P1, device, "OT"))),
                    "device " + device);
        }
    }

    // ── 6. 大规模：批量任务重推完整且不跨项目泄漏 ──────────────────────

    @Test
    void largeScaleReSyncIsProjectScopedAndComplete() {
        buildWorld(P1, 3, List.of("OT", "HUFL"));
        buildWorld(P2, 2, List.of("OT"));

        int perProject = 200;
        for (int i = 0; i < perProject; i++) {
            cache.putAnomalyTask(anomalyTask(P1, "p1-" + i,
                    List.of(seqId(P1, 1 + (i % 3), "OT")), List.of()));
            cache.putAnomalyTask(anomalyTask(P2, "p2-" + i,
                    List.of(seqId(P2, 1 + (i % 2), "OT")), List.of()));
        }

        clearInvocations(anomalyClient, forecastClient);
        service.saveConstraint(constraintRequest(P1, "c-bulk", "bulk", Map.of("x", "OT"), "x <= 100"));

        ArgumentCaptor<TimeseriesAnomalyTask> captor = ArgumentCaptor.forClass(TimeseriesAnomalyTask.class);
        verify(anomalyClient, times(perProject)).syncAnomalyTask(captor.capture());
        List<TimeseriesAnomalyTask> reSynced = captor.getAllValues();
        assertEquals(perProject, reSynced.size());
        assertEquals(perProject,
                reSynced.stream().map(TimeseriesAnomalyTask::getTaskId).collect(Collectors.toSet()).size(),
                "同一任务不应被重复重推");
        assertTrue(reSynced.stream().allMatch(t -> P1.equals(t.getProjectId())),
                "P2 任务不应被 P1 约束变更重推");
        assertTrue(reSynced.stream().allMatch(t -> t.getTaskId().startsWith("p1-")),
                "重推的任务应全部来自 P1");
    }

    // ── helpers ──────────────────────────────────────────────────────

    private static String seqId(String projectId, int device, String category) {
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
}
