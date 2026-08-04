package com.sfkg.timeseries.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sfkg.timeseries.config.GrpcClientProperties;
import com.sfkg.timeseries.dto.HistoryDataQueryRequest;
import com.sfkg.timeseries.dto.SyncResult;
import com.sfkg.timeseries.entity.TimeseriesAnomalyTask;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.entity.TimeseriesDataPoint;
import com.sfkg.timeseries.entity.TimeseriesForecastTask;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.entity.TimeseriesRelation;
import com.sfkg.timeseries.grpc.QueryHistoryDataRequest;
import com.sfkg.timeseries.grpc.QueryHistoryDataResponse;
import com.sfkg.timeseries.grpc.SyncConstraintConfigRequest;
import com.sfkg.timeseries.grpc.SyncInstanceConfigRequest;
import com.sfkg.timeseries.grpc.SyncRelationConfigRequest;
import com.sfkg.timeseries.grpc.SyncResponse;
import com.sfkg.timeseries.grpc.SyncTaskStatusRequest;
import com.sfkg.timeseries.grpc.SyncTimeseriesDataRequest;
import com.sfkg.timeseries.grpc.TimeseriesCoreServiceGrpc;
import com.sfkg.timeseries.grpc.TimeseriesDataPointMessage;
import com.sfkg.timeseries.vo.HistoryDataVO;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
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

    public TimeseriesCoreGrpcClient(GrpcClientProperties grpcClientProperties, ObjectMapper objectMapper) {
        this.grpcClientProperties = grpcClientProperties;
        this.objectMapper = objectMapper;
    }

    // ── instance config ────────────────────────────────────────────────

    public SyncResult syncInstanceConfig(TimeseriesInstanceConfig config) {
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return notConfigured("syncInstanceConfig");
        }
        if (config == null) {
            return SyncResult.fail("config is null");
        }
        SyncInstanceConfigRequest req = SyncInstanceConfigRequest.newBuilder()
                .setSequenceId(nullToZero(config.getSequenceId()))
                .setInstanceName(nullToEmpty(config.getInstanceName()))
                .setCategoryId(nullToZero(config.getCategoryId()))
                .setDeviceInstanceId(nullToZero(config.getDeviceInstanceId()))
                .build();
        LOG.info("[{}] -> syncInstanceConfig sequenceId={} at {}", SERVICE_NAME, config.getSequenceId(), address);
        return callCore(address, stub -> stub.syncInstanceConfig(req), "syncInstanceConfig");
    }

    // ── constraint config ──────────────────────────────────────────────

    public SyncResult syncConstraintConfig(TimeseriesConstraint constraint) {
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return notConfigured("syncConstraintConfig");
        }
        if (constraint == null) {
            return SyncResult.fail("constraint is null");
        }
        SyncConstraintConfigRequest.Builder b = SyncConstraintConfigRequest.newBuilder()
                .setConstraintId(nullToZero(constraint.getConstraintId()))
                .setConstraintExpression(nullToEmpty(constraint.getConstraintExpression()));
        if (constraint.getVariableMapping() != null) {
            b.putAllVariableMapping(constraint.getVariableMapping());
        }
        LOG.info("[{}] -> syncConstraintConfig constraintId={} at {}", SERVICE_NAME, constraint.getConstraintId(), address);
        return callCore(address, stub -> stub.syncConstraintConfig(b.build()), "syncConstraintConfig");
    }

    // ── relation config ────────────────────────────────────────────────

    public SyncResult syncRelationConfig(TimeseriesRelation relation) {
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return notConfigured("syncRelationConfig");
        }
        if (relation == null) {
            return SyncResult.fail("relation is null");
        }
        SyncRelationConfigRequest.Builder b = SyncRelationConfigRequest.newBuilder()
                .setRelationId(nullToZero(relation.getRelationId()))
                .setTargetCategoryId(nullToZero(relation.getTargetCategoryId()));
        if (relation.getSourceCategories() != null) {
            b.addAllSourceCategories(relation.getSourceCategories());
        }
        LOG.info("[{}] -> syncRelationConfig relationId={} at {}", SERVICE_NAME, relation.getRelationId(), address);
        return callCore(address, stub -> stub.syncRelationConfig(b.build()), "syncRelationConfig");
    }

    // ── anomaly / forecast task config (no dedicated core RPC yet) ────

    public SyncResult syncAnomalyTaskConfig(TimeseriesAnomalyTask task) {
        LOG.info("[{}] syncAnomalyTaskConfig taskId={} - using syncTaskStatus fallback", SERVICE_NAME,
                task != null ? task.getTaskId() : null);
        if (task == null) {
            return SyncResult.fail("task is null");
        }
        return updateTaskStatus(task.getTaskId(), "ANOMALY", task.getStatus());
    }

    public SyncResult syncForecastTaskConfig(TimeseriesForecastTask task) {
        LOG.info("[{}] syncForecastTaskConfig taskId={} - using syncTaskStatus fallback", SERVICE_NAME,
                task != null ? task.getTaskId() : null);
        if (task == null) {
            return SyncResult.fail("task is null");
        }
        return updateTaskStatus(task.getTaskId(), "FORECAST", task.getStatus());
    }

    // ── task status ────────────────────────────────────────────────────

    public SyncResult updateTaskStatus(Integer taskId, String taskType, String status) {
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return notConfigured("updateTaskStatus");
        }
        SyncTaskStatusRequest req = SyncTaskStatusRequest.newBuilder()
                .setTaskId(nullToZero(taskId))
                .setTaskType(nullToEmpty(taskType))
                .setStatus(nullToEmpty(status))
                .build();
        LOG.info("[{}] -> updateTaskStatus taskId={} type={} status={} at {}", SERVICE_NAME, taskId, taskType, status, address);
        return callCore(address, stub -> stub.syncTaskStatus(req), "updateTaskStatus");
    }

    // ── timeseries data ────────────────────────────────────────────────

    public SyncResult syncTimeseriesData(List<TimeseriesDataPoint> points) {
        if (points == null || points.isEmpty()) {
            return SyncResult.success();
        }
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return notConfigured("syncTimeseriesData");
        }

        SyncTimeseriesDataRequest.Builder b = SyncTimeseriesDataRequest.newBuilder()
                .setSequenceId(nullToZero(points.get(0).getSequenceId()));
        for (TimeseriesDataPoint point : points) {
            b.addPoints(TimeseriesDataPointMessage.newBuilder()
                    .setSequenceId(nullToZero(point.getSequenceId()))
                    .setTimestamp(point.getTimestamp() != null ? point.getTimestamp().toString() : "")
                    .setValue(point.getValue() != null ? point.getValue().toPlainString() : "0")
                    .build());
        }
        LOG.info("[{}] -> syncTimeseriesData seq={} points={} at {}", SERVICE_NAME,
                points.get(0).getSequenceId(), points.size(), address);
        return callCore(address, stub -> stub.syncTimeseriesData(b.build()), "syncTimeseriesData");
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
        QueryHistoryDataRequest req = QueryHistoryDataRequest.newBuilder()
                .setSequenceId(nullToZero(request.getSequenceId()))
                .setStartTime(request.getStartTime() != null ? request.getStartTime().toString() : "")
                .setEndTime(request.getEndTime() != null ? request.getEndTime().toString() : "")
                .setGranularity(nullToEmpty(request.getGranularity()))
                .build();
        LOG.info("[{}] -> queryHistoryData seq={} at {}", SERVICE_NAME, request.getSequenceId(), address);

        ManagedChannel channel = newChannel(address);
        try {
            QueryHistoryDataResponse resp = TimeseriesCoreServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(3, TimeUnit.SECONDS)
                    .queryHistoryData(req);
            LOG.info("[{}] <- queryHistoryData seq={} returned payload {} chars",
                    SERVICE_NAME, resp.getSequenceId(),
                    resp.getPayloadJson() != null ? resp.getPayloadJson().length() : 0);
            HistoryDataVO vo = new HistoryDataVO();
            vo.setSequenceId(resp.getSequenceId());
            if (resp.getPayloadJson() != null && !resp.getPayloadJson().isBlank()) {
                try {
                    HistoryDataVO parsed = objectMapper.readValue(resp.getPayloadJson(), HistoryDataVO.class);
                    vo.setPoints(parsed.getPoints());
                } catch (JsonProcessingException e) {
                    LOG.warn("[{}] failed to parse history data payload", SERVICE_NAME, e);
                }
            }
            return vo;
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] queryHistoryData failed: {}", SERVICE_NAME, e.getStatus().getDescription());
            return new HistoryDataVO();
        } finally {
            channel.shutdown();
        }
    }

    // ── placeholder stubs (no proto RPC defined yet) ───────────────────

    public Map<String, Object> queryWindowData(Integer sequenceId) {
        LOG.debug("[{}] queryWindowData seq={} - stub", SERVICE_NAME, sequenceId);
        return Map.of();
    }

    public Map<String, Object> queryStatistics(Integer sequenceId) {
        LOG.debug("[{}] queryStatistics seq={} - stub", SERVICE_NAME, sequenceId);
        return Map.of();
    }

    // ── internal helpers ───────────────────────────────────────────────

    private ManagedChannel newChannel(String address) {
        return ManagedChannelBuilder.forTarget(address).usePlaintext().build();
    }

    @FunctionalInterface
    private interface CoreStubCall {
        SyncResponse call(TimeseriesCoreServiceGrpc.TimeseriesCoreServiceBlockingStub stub);
    }

    private SyncResult callCore(String address, CoreStubCall callable, String operation) {
        ManagedChannel channel = newChannel(address);
        try {
            SyncResponse resp = callable.call(
                    TimeseriesCoreServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(3, TimeUnit.SECONDS));
            LOG.info("[{}] <- {} success={} msg={}", SERVICE_NAME, operation, resp.getSuccess(), resp.getMessage());
            return SyncResult.of(resp.getSuccess(), resp.getMessage());
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] <- {} FAILED: {}", SERVICE_NAME, operation, e.getStatus().getDescription());
            return SyncResult.fail(e.getStatus().getDescription());
        } finally {
            channel.shutdown();
        }
    }

    private SyncResult notConfigured(String operation) {
        LOG.info("[{}] {} skipped: core address not configured", SERVICE_NAME, operation);
        return SyncResult.fail("core address not configured");
    }

    private static int nullToZero(Integer v) { return v != null ? v : 0; }
    private static String nullToEmpty(String v) { return v != null ? v : ""; }
    private static boolean isBlank(String s) { return s == null || s.isBlank(); }
}
