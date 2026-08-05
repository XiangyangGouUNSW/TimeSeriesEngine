package com.sfkg.timeseries.client;

import com.sfkg.timeseries.config.GrpcClientProperties;
import com.sfkg.timeseries.dto.ForecastResultQueryRequest;
import com.sfkg.timeseries.dto.SyncResult;
import com.sfkg.timeseries.entity.TimeseriesForecastTask;
import com.sfkg.timeseries.grpc.AnalysisSyncForecastTaskRequest;
import com.sfkg.timeseries.grpc.AnalysisTaskStatus;
import com.sfkg.timeseries.grpc.AnalysisUpdateTaskStatusRequest;
import com.sfkg.timeseries.grpc.ForecastTaskConfig;
import com.sfkg.timeseries.grpc.QueryForecastResultsRequest;
import com.sfkg.timeseries.grpc.QueryForecastResultsResponse;
import com.sfkg.timeseries.grpc.RequestMeta;
import com.sfkg.timeseries.grpc.ResultQuery;
import com.sfkg.timeseries.grpc.TaskAck;
import com.sfkg.timeseries.grpc.TimeseriesAnalysisServiceGrpc;
import com.sfkg.timeseries.vo.ForecastResultVO;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ForecastGrpcClient {

    private static final Logger LOG = LoggerFactory.getLogger(ForecastGrpcClient.class);
    private static final String SERVICE_NAME = "timeseries-analysis";

    private final GrpcClientProperties grpcClientProperties;

    public ForecastGrpcClient(GrpcClientProperties grpcClientProperties) {
        this.grpcClientProperties = grpcClientProperties;
    }

    public SyncResult syncForecastTask(TimeseriesForecastTask task) {
        String address = grpcClientProperties.getForecastAddress();
        if (isBlank(address)) {
            return notConfigured("SyncForecastTask");
        }
        if (task == null) {
            return SyncResult.fail("task is null");
        }
        ForecastTaskConfig.Builder configBuilder = ForecastTaskConfig.newBuilder()
                .setTaskId(nullToEmpty(task.getTaskId()))
                .setTaskName(nullToEmpty(task.getTaskName()));
        if (task.getForecastObjects() != null) {
            configBuilder.addAllTargetSequenceIds(task.getForecastObjects());
        }
        if (task.getForecastHorizon() != null) {
            try {
                configBuilder.setForecastHorizonSteps(Integer.parseInt(task.getForecastHorizon()));
            } catch (NumberFormatException ignored) {}
        }

        AnalysisSyncForecastTaskRequest req = AnalysisSyncForecastTaskRequest.newBuilder()
                .setMeta(newMeta())
                .setTask(configBuilder.build())
                .build();

        LOG.info("[{}] -> SyncForecastTask taskId={} at {}", SERVICE_NAME, task.getTaskId(), address);
        ManagedChannel channel = newChannel(address);
        try {
            TaskAck ack = TimeseriesAnalysisServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .syncForecastTask(req);
            LOG.info("[{}] <- SyncForecastTask accepted={} msg={}", SERVICE_NAME, ack.getAccepted(), ack.getMessage());
            return SyncResult.of(ack.getAccepted(), ack.getMessage());
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] <- SyncForecastTask FAILED: {}", SERVICE_NAME, e.getStatus().getDescription());
            return SyncResult.fail(e.getStatus().getDescription());
        } finally {
            channel.shutdown();
        }
    }

    public SyncResult updateForecastTaskStatus(String taskId, String status) {
        String address = grpcClientProperties.getForecastAddress();
        if (isBlank(address)) {
            return notConfigured("UpdateTaskStatus");
        }
        AnalysisTaskStatus protoStatus;
        if ("ENABLE".equalsIgnoreCase(status) || "ENABLED".equalsIgnoreCase(status)) {
            protoStatus = AnalysisTaskStatus.TASK_STATUS_ENABLED;
        } else if ("DISABLE".equalsIgnoreCase(status) || "DISABLED".equalsIgnoreCase(status)) {
            protoStatus = AnalysisTaskStatus.TASK_STATUS_DISABLED;
        } else {
            protoStatus = AnalysisTaskStatus.TASK_STATUS_UNSPECIFIED;
        }
        AnalysisUpdateTaskStatusRequest req = AnalysisUpdateTaskStatusRequest.newBuilder()
                .setMeta(newMeta())
                .setTaskId(nullToEmpty(taskId))
                .setStatus(protoStatus)
                .build();
        LOG.info("[{}] -> UpdateTaskStatus taskId={} status={} at {}", SERVICE_NAME, taskId, status, address);
        ManagedChannel channel = newChannel(address);
        try {
            TaskAck ack = TimeseriesAnalysisServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .updateTaskStatus(req);
            return SyncResult.of(ack.getAccepted(), ack.getMessage());
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] <- UpdateTaskStatus FAILED: {}", SERVICE_NAME, e.getStatus().getDescription());
            return SyncResult.fail(e.getStatus().getDescription());
        } finally {
            channel.shutdown();
        }
    }

    public ForecastResultVO queryForecastResult(ForecastResultQueryRequest request) {
        String address = grpcClientProperties.getForecastAddress();
        if (isBlank(address)) {
            LOG.info("[{}] queryForecastResults skipped: address not configured", SERVICE_NAME);
            return new ForecastResultVO();
        }
        if (request == null) {
            return new ForecastResultVO();
        }
        QueryForecastResultsRequest req = QueryForecastResultsRequest.newBuilder()
                .setQuery(ResultQuery.newBuilder()
                        .setMeta(newMeta())
                        .setTaskId(nullToEmpty(request.getTaskId()))
                        .setLatestOnly(true)
                        .setLimit(10)
                        .build())
                .build();
        LOG.info("[{}] -> QueryForecastResults taskId={} at {}", SERVICE_NAME, request.getTaskId(), address);
        ManagedChannel channel = newChannel(address);
        try {
            QueryForecastResultsResponse resp = TimeseriesAnalysisServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .queryForecastResults(req);
            ForecastResultVO vo = new ForecastResultVO();
            vo.setTaskId(resp.getTaskId());
            if (resp.getResultsCount() > 0) {
                vo.setResultId(resp.getResults(0).getRunId());
            }
            return vo;
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] queryForecastResults failed: {}", SERVICE_NAME, e.getStatus().getDescription());
            return new ForecastResultVO();
        } finally {
            channel.shutdown();
        }
    }

    // ── helpers ────────────────────────────────────────────────────────

    private ManagedChannel newChannel(String address) {
        return ManagedChannelBuilder.forTarget(address).usePlaintext().build();
    }

    private RequestMeta newMeta() {
        return RequestMeta.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setSentAtMs(System.currentTimeMillis())
                .build();
    }

    private SyncResult notConfigured(String operation) {
        LOG.info("[{}] {} skipped: forecast address not configured", SERVICE_NAME, operation);
        return SyncResult.fail("forecast address not configured");
    }

    private static String nullToEmpty(String v) { return v != null ? v : ""; }
    private static boolean isBlank(String s) { return s == null || s.isBlank(); }
}
