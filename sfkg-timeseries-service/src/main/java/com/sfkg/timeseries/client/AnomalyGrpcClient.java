package com.sfkg.timeseries.client;

import com.sfkg.timeseries.config.GrpcClientProperties;
import com.sfkg.timeseries.dto.AnomalyResultQueryRequest;
import com.sfkg.timeseries.dto.SyncResult;
import com.sfkg.timeseries.entity.TimeseriesAnomalyTask;
import com.sfkg.timeseries.grpc.AnalysisStatus;
import com.sfkg.timeseries.grpc.AnalysisSyncAnomalyTaskRequest;
import com.sfkg.timeseries.grpc.AnalysisTaskStatus;
import com.sfkg.timeseries.grpc.AnalysisUpdateTaskStatusRequest;
import com.sfkg.timeseries.grpc.AnomalyTaskConfig;
import com.sfkg.timeseries.grpc.QueryAnomalyResultsRequest;
import com.sfkg.timeseries.grpc.QueryAnomalyResultsResponse;
import com.sfkg.timeseries.grpc.RequestMeta;
import com.sfkg.timeseries.grpc.ResultQuery;
import com.sfkg.timeseries.grpc.SemanticContext;
import com.sfkg.timeseries.grpc.TaskAck;
import com.sfkg.timeseries.grpc.TimeseriesAnalysisServiceGrpc;
import com.sfkg.timeseries.vo.AnomalyResultVO;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class AnomalyGrpcClient {

    private static final Logger LOG = LoggerFactory.getLogger(AnomalyGrpcClient.class);
    private static final String SERVICE_NAME = "timeseries-analysis";

    private final GrpcClientProperties grpcClientProperties;

    public AnomalyGrpcClient(GrpcClientProperties grpcClientProperties) {
        this.grpcClientProperties = grpcClientProperties;
    }

    public SyncResult syncAnomalyTask(TimeseriesAnomalyTask task) {
        String address = grpcClientProperties.getAnomalyAddress();
        if (isBlank(address)) {
            return notConfigured("SyncAnomalyTask");
        }
        if (task == null) {
            return SyncResult.fail("task is null");
        }

        AnomalyTaskConfig.Builder configBuilder = AnomalyTaskConfig.newBuilder()
                .setTaskId(nullToEmpty(task.getTaskId()))
                .setTaskName(nullToEmpty(task.getTaskName()))
                .setWarningRule(nullToEmpty(task.getWarningRule()));
        if (task.getSequenceIds() != null) {
            configBuilder.addAllSequenceIds(task.getSequenceIds());
        }
        if (task.getMethods() != null) {
            configBuilder.addAllMethods(task.getMethods());
        }
        if (task.getContextLength() != null) {
            configBuilder.setContextLength(task.getContextLength());
        }
        if (task.getSlideStepMs() != null) {
            configBuilder.setSlideStepMs(task.getSlideStepMs());
        }
        if (task.getMinimumPoints() != null) {
            configBuilder.setMinimumPoints(task.getMinimumPoints());
        }
        if (task.getConstraintIds() != null && !task.getConstraintIds().isEmpty()) {
            configBuilder.setSemanticContext(
                    SemanticContext.newBuilder().addAllConstraintIds(task.getConstraintIds()).build());
        }

        AnalysisSyncAnomalyTaskRequest req = AnalysisSyncAnomalyTaskRequest.newBuilder()
                .setMeta(newMeta())
                .setConfigVersion(System.currentTimeMillis())
                .setTask(configBuilder.build())
                .build();

        LOG.info("[{}] -> SyncAnomalyTask taskId={} at {}", SERVICE_NAME, task.getTaskId(), address);
        return callSyncTask(address, req);
    }

    public SyncResult updateAnomalyTaskStatus(String taskId, String status) {
        String address = grpcClientProperties.getAnomalyAddress();
        if (isBlank(address)) {
            return notConfigured("UpdateTaskStatus");
        }

        AnalysisTaskStatus protoStatus = mapTaskStatus(status);
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
            LOG.info("[{}] <- UpdateTaskStatus accepted={} msg={}", SERVICE_NAME, ack.getAccepted(), ack.getMessage());
            return SyncResult.of(ack.getAccepted(), ack.getMessage());
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] <- UpdateTaskStatus FAILED: {}", SERVICE_NAME, e.getStatus().getDescription());
            return SyncResult.fail(e.getStatus().getDescription());
        } finally {
            channel.shutdown();
        }
    }

    public AnomalyResultVO queryAnomalyResult(AnomalyResultQueryRequest request) {
        String address = grpcClientProperties.getAnomalyAddress();
        if (isBlank(address)) {
            LOG.info("[{}] queryAnomalyResults skipped: address not configured", SERVICE_NAME);
            return new AnomalyResultVO();
        }
        if (request == null) {
            return new AnomalyResultVO();
        }

        ResultQuery.Builder queryBuilder = ResultQuery.newBuilder()
                .setMeta(newMeta())
                .setTaskId(nullToEmpty(request.getTaskId()))
                .setLatestOnly(true)
                .setLimit(10);

        QueryAnomalyResultsRequest req = QueryAnomalyResultsRequest.newBuilder()
                .setQuery(queryBuilder.build())
                .build();

        LOG.info("[{}] -> QueryAnomalyResults taskId={} at {}", SERVICE_NAME, request.getTaskId(), address);
        ManagedChannel channel = newChannel(address);
        try {
            QueryAnomalyResultsResponse resp = TimeseriesAnalysisServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .queryAnomalyResults(req);
            LOG.info("[{}] <- QueryAnomalyResults taskId={} results={}", SERVICE_NAME,
                    resp.getTaskId(), resp.getResultsCount());
            AnomalyResultVO vo = new AnomalyResultVO();
            vo.setTaskId(resp.getTaskId());
            if (resp.getResultsCount() > 0) {
                var first = resp.getResults(0);
                vo.setResultId(first.getRunId());
                if (first.getFindingsCount() > 0) {
                    vo.setAnomalyLevel(first.getFindings(0).getSeverity());
                }
            }
            return vo;
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] queryAnomalyResults failed: {}", SERVICE_NAME, e.getStatus().getDescription());
            return new AnomalyResultVO();
        } finally {
            channel.shutdown();
        }
    }

    // ── helpers ────────────────────────────────────────────────────────

    private SyncResult callSyncTask(String address, AnalysisSyncAnomalyTaskRequest req) {
        ManagedChannel channel = newChannel(address);
        try {
            TaskAck ack = TimeseriesAnalysisServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .syncAnomalyTask(req);
            LOG.info("[{}] <- SyncAnomalyTask accepted={} status={} msg={}",
                    SERVICE_NAME, ack.getAccepted(), ack.getStatus(), ack.getMessage());
            return SyncResult.of(ack.getAccepted(), ack.getMessage());
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] <- SyncAnomalyTask FAILED: {}", SERVICE_NAME, e.getStatus().getDescription());
            return SyncResult.fail(e.getStatus().getDescription());
        } finally {
            channel.shutdown();
        }
    }

    private ManagedChannel newChannel(String address) {
        return ManagedChannelBuilder.forTarget(address).usePlaintext().build();
    }

    private RequestMeta newMeta() {
        return RequestMeta.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setSentAtMs(System.currentTimeMillis())
                .build();
    }

    private AnalysisTaskStatus mapTaskStatus(String status) {
        if (status == null) return AnalysisTaskStatus.TASK_STATUS_UNSPECIFIED;
        switch (status.toUpperCase()) {
            case "ENABLE": case "ENABLED": return AnalysisTaskStatus.TASK_STATUS_ENABLED;
            case "DISABLE": case "DISABLED": return AnalysisTaskStatus.TASK_STATUS_DISABLED;
            case "DELETE": case "DELETED": return AnalysisTaskStatus.TASK_STATUS_DELETED;
            default: return AnalysisTaskStatus.TASK_STATUS_UNSPECIFIED;
        }
    }

    private SyncResult notConfigured(String operation) {
        LOG.info("[{}] {} skipped: analysis address not configured", SERVICE_NAME, operation);
        return SyncResult.fail("analysis address not configured");
    }

    private static String nullToEmpty(String v) { return v != null ? v : ""; }
    private static boolean isBlank(String s) { return s == null || s.isBlank(); }
}
