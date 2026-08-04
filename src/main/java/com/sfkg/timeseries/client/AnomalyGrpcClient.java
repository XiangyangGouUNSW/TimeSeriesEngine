package com.sfkg.timeseries.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sfkg.timeseries.config.GrpcClientProperties;
import com.sfkg.timeseries.dto.AnomalyResultQueryRequest;
import com.sfkg.timeseries.dto.SyncResult;
import com.sfkg.timeseries.entity.TimeseriesAnomalyTask;
import com.sfkg.timeseries.grpc.QueryAnomalyResultRequest;
import com.sfkg.timeseries.grpc.QueryAnomalyResultResponse;
import com.sfkg.timeseries.grpc.SyncAnomalyTaskRequest;
import com.sfkg.timeseries.grpc.SyncResponse;
import com.sfkg.timeseries.grpc.SyncTaskStatusRequest;
import com.sfkg.timeseries.grpc.TimeseriesAnomalyServiceGrpc;
import com.sfkg.timeseries.vo.AnomalyResultVO;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class AnomalyGrpcClient {

    private static final Logger LOG = LoggerFactory.getLogger(AnomalyGrpcClient.class);
    private static final String SERVICE_NAME = "timeseries-anomaly";

    private final GrpcClientProperties grpcClientProperties;
    private final ObjectMapper objectMapper;

    public AnomalyGrpcClient(GrpcClientProperties grpcClientProperties, ObjectMapper objectMapper) {
        this.grpcClientProperties = grpcClientProperties;
        this.objectMapper = objectMapper;
    }

    public SyncResult syncAnomalyTask(TimeseriesAnomalyTask task) {
        String address = grpcClientProperties.getAnomalyAddress();
        if (isBlank(address)) {
            return notConfigured("syncAnomalyTask");
        }
        if (task == null) {
            return SyncResult.fail("task is null");
        }
        SyncAnomalyTaskRequest.Builder b = SyncAnomalyTaskRequest.newBuilder()
                .setTaskId(nullToZero(task.getTaskId()))
                .setDetectMethod(nullToEmpty(task.getDetectMethod()));
        if (task.getDetectObjects() != null) {
            b.addAllDetectObjects(task.getDetectObjects());
        }
        LOG.info("[{}] -> syncAnomalyTask taskId={} method={} at {}", SERVICE_NAME, task.getTaskId(), task.getDetectMethod(), address);
        return callAnomaly(address, stub -> stub.syncAnomalyTask(b.build()), "syncAnomalyTask");
    }

    public SyncResult updateAnomalyTaskStatus(Integer taskId, String status) {
        String address = grpcClientProperties.getAnomalyAddress();
        if (isBlank(address)) {
            return notConfigured("updateAnomalyTaskStatus");
        }
        SyncTaskStatusRequest req = SyncTaskStatusRequest.newBuilder()
                .setTaskId(nullToZero(taskId))
                .setTaskType("ANOMALY")
                .setStatus(nullToEmpty(status))
                .build();
        LOG.info("[{}] -> updateAnomalyTaskStatus taskId={} status={} at {}", SERVICE_NAME, taskId, status, address);
        return callAnomaly(address, stub -> stub.updateAnomalyTaskStatus(req), "updateAnomalyTaskStatus");
    }

    public AnomalyResultVO queryAnomalyResult(AnomalyResultQueryRequest request) {
        String address = grpcClientProperties.getAnomalyAddress();
        if (isBlank(address)) {
            LOG.info("[{}] queryAnomalyResult skipped: address not configured", SERVICE_NAME);
            return new AnomalyResultVO();
        }
        if (request == null) {
            return new AnomalyResultVO();
        }
        QueryAnomalyResultRequest req = QueryAnomalyResultRequest.newBuilder()
                .setTaskId(nullToZero(request.getTaskId()))
                .setSequenceId(nullToZero(request.getSequenceId()))
                .build();
        LOG.info("[{}] -> queryAnomalyResult taskId={} seq={} at {}", SERVICE_NAME, request.getTaskId(), request.getSequenceId(), address);

        ManagedChannel channel = newChannel(address);
        try {
            QueryAnomalyResultResponse resp = TimeseriesAnomalyServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(3, TimeUnit.SECONDS)
                    .queryAnomalyResult(req);
            LOG.info("[{}] <- queryAnomalyResult payload {} chars", SERVICE_NAME,
                    resp.getPayloadJson() != null ? resp.getPayloadJson().length() : 0);
            AnomalyResultVO vo = new AnomalyResultVO();
            vo.setTaskId(request.getTaskId());
            vo.setSequenceId(request.getSequenceId());
            if (resp.getPayloadJson() != null && !resp.getPayloadJson().isBlank()) {
                try {
                    @SuppressWarnings("unchecked")
                    java.util.Map<String, Object> map = objectMapper.readValue(resp.getPayloadJson(), java.util.Map.class);
                    vo.setResultId(map.get("resultId") instanceof Number n ? n.intValue() : null);
                    vo.setAnomalyLevel((String) map.get("anomalyLevel"));
                } catch (JsonProcessingException e) {
                    LOG.warn("[{}] failed to parse anomaly result payload", SERVICE_NAME, e);
                }
            }
            return vo;
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] queryAnomalyResult failed: {}", SERVICE_NAME, e.getStatus().getDescription());
            return new AnomalyResultVO();
        } finally {
            channel.shutdown();
        }
    }

    // ── helpers ────────────────────────────────────────────────────────

    private ManagedChannel newChannel(String address) {
        return ManagedChannelBuilder.forTarget(address).usePlaintext().build();
    }

    @FunctionalInterface
    private interface AnomalyStubCall {
        SyncResponse call(TimeseriesAnomalyServiceGrpc.TimeseriesAnomalyServiceBlockingStub stub);
    }

    private SyncResult callAnomaly(String address, AnomalyStubCall callable, String operation) {
        ManagedChannel channel = newChannel(address);
        try {
            SyncResponse resp = callable.call(
                    TimeseriesAnomalyServiceGrpc.newBlockingStub(channel)
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
        LOG.info("[{}] {} skipped: anomaly address not configured", SERVICE_NAME, operation);
        return SyncResult.fail("anomaly address not configured");
    }

    private static int nullToZero(Integer v) { return v != null ? v : 0; }
    private static String nullToEmpty(String v) { return v != null ? v : ""; }
    private static boolean isBlank(String s) { return s == null || s.isBlank(); }
}
