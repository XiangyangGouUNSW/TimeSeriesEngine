package com.sfkg.timeseries.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sfkg.timeseries.config.GrpcClientProperties;
import com.sfkg.timeseries.dto.ForecastResultQueryRequest;
import com.sfkg.timeseries.dto.SyncResult;
import com.sfkg.timeseries.entity.TimeseriesForecastTask;
import com.sfkg.timeseries.grpc.QueryForecastResultRequest;
import com.sfkg.timeseries.grpc.QueryForecastResultResponse;
import com.sfkg.timeseries.grpc.SyncForecastTaskRequest;
import com.sfkg.timeseries.grpc.SyncResponse;
import com.sfkg.timeseries.grpc.SyncTaskStatusRequest;
import com.sfkg.timeseries.grpc.TimeseriesForecastServiceGrpc;
import com.sfkg.timeseries.vo.ForecastResultVO;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ForecastGrpcClient {

    private static final Logger LOG = LoggerFactory.getLogger(ForecastGrpcClient.class);
    private static final String SERVICE_NAME = "timeseries-forecast";

    private final GrpcClientProperties grpcClientProperties;
    private final ObjectMapper objectMapper;

    public ForecastGrpcClient(GrpcClientProperties grpcClientProperties, ObjectMapper objectMapper) {
        this.grpcClientProperties = grpcClientProperties;
        this.objectMapper = objectMapper;
    }

    public SyncResult syncForecastTask(TimeseriesForecastTask task) {
        String address = grpcClientProperties.getForecastAddress();
        if (isBlank(address)) {
            return notConfigured("syncForecastTask");
        }
        if (task == null) {
            return SyncResult.fail("task is null");
        }
        SyncForecastTaskRequest.Builder b = SyncForecastTaskRequest.newBuilder()
                .setTaskId(nullToZero(task.getTaskId()))
                .setForecastHorizon(nullToEmpty(task.getForecastHorizon()));
        if (task.getForecastObjects() != null) {
            b.addAllForecastObjects(task.getForecastObjects());
        }
        LOG.info("[{}] -> syncForecastTask taskId={} horizon={} at {}", SERVICE_NAME, task.getTaskId(), task.getForecastHorizon(), address);
        return callForecast(address, stub -> stub.syncForecastTask(b.build()), "syncForecastTask");
    }

    public SyncResult updateForecastTaskStatus(Integer taskId, String status) {
        String address = grpcClientProperties.getForecastAddress();
        if (isBlank(address)) {
            return notConfigured("updateForecastTaskStatus");
        }
        SyncTaskStatusRequest req = SyncTaskStatusRequest.newBuilder()
                .setTaskId(nullToZero(taskId))
                .setTaskType("FORECAST")
                .setStatus(nullToEmpty(status))
                .build();
        LOG.info("[{}] -> updateForecastTaskStatus taskId={} status={} at {}", SERVICE_NAME, taskId, status, address);
        return callForecast(address, stub -> stub.updateForecastTaskStatus(req), "updateForecastTaskStatus");
    }

    public ForecastResultVO queryForecastResult(ForecastResultQueryRequest request) {
        String address = grpcClientProperties.getForecastAddress();
        if (isBlank(address)) {
            LOG.info("[{}] queryForecastResult skipped: address not configured", SERVICE_NAME);
            return new ForecastResultVO();
        }
        if (request == null) {
            return new ForecastResultVO();
        }
        QueryForecastResultRequest req = QueryForecastResultRequest.newBuilder()
                .setTaskId(nullToZero(request.getTaskId()))
                .setSequenceId(nullToZero(request.getSequenceId()))
                .build();
        LOG.info("[{}] -> queryForecastResult taskId={} seq={} at {}", SERVICE_NAME, request.getTaskId(), request.getSequenceId(), address);

        ManagedChannel channel = newChannel(address);
        try {
            QueryForecastResultResponse resp = TimeseriesForecastServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(3, TimeUnit.SECONDS)
                    .queryForecastResult(req);
            LOG.info("[{}] <- queryForecastResult payload {} chars", SERVICE_NAME,
                    resp.getPayloadJson() != null ? resp.getPayloadJson().length() : 0);
            ForecastResultVO vo = new ForecastResultVO();
            vo.setTaskId(request.getTaskId());
            vo.setSequenceId(request.getSequenceId());
            if (resp.getPayloadJson() != null && !resp.getPayloadJson().isBlank()) {
                try {
                    @SuppressWarnings("unchecked")
                    java.util.Map<String, Object> map = objectMapper.readValue(resp.getPayloadJson(), java.util.Map.class);
                    vo.setResultId(map.get("resultId") instanceof Number n ? n.intValue() : null);
                    vo.setWarningLevel((String) map.get("warningLevel"));
                } catch (JsonProcessingException e) {
                    LOG.warn("[{}] failed to parse forecast result payload", SERVICE_NAME, e);
                }
            }
            return vo;
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] queryForecastResult failed: {}", SERVICE_NAME, e.getStatus().getDescription());
            return new ForecastResultVO();
        } finally {
            channel.shutdown();
        }
    }

    // ── helpers ────────────────────────────────────────────────────────

    private ManagedChannel newChannel(String address) {
        return ManagedChannelBuilder.forTarget(address).usePlaintext().build();
    }

    @FunctionalInterface
    private interface ForecastStubCall {
        SyncResponse call(TimeseriesForecastServiceGrpc.TimeseriesForecastServiceBlockingStub stub);
    }

    private SyncResult callForecast(String address, ForecastStubCall callable, String operation) {
        ManagedChannel channel = newChannel(address);
        try {
            SyncResponse resp = callable.call(
                    TimeseriesForecastServiceGrpc.newBlockingStub(channel)
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
        LOG.info("[{}] {} skipped: forecast address not configured", SERVICE_NAME, operation);
        return SyncResult.fail("forecast address not configured");
    }

    private static int nullToZero(Integer v) { return v != null ? v : 0; }
    private static String nullToEmpty(String v) { return v != null ? v : ""; }
    private static boolean isBlank(String s) { return s == null || s.isBlank(); }
}
