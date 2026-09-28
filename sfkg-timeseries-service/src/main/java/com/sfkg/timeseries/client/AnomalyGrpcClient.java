package com.sfkg.timeseries.client;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.sfkg.timeseries.config.GrpcClientProperties;
import com.sfkg.timeseries.config.RetryPolicyProperties;
import com.sfkg.timeseries.dto.AnomalyResultQueryRequest;
import com.sfkg.timeseries.dto.SyncResult;
import com.sfkg.timeseries.entity.TimeseriesAnomalyTask;
import com.sfkg.timeseries.grpc.AnalysisSyncAnomalyTaskRequest;
import com.sfkg.timeseries.grpc.AnalysisTaskStatus;
import com.sfkg.timeseries.grpc.AnalysisUpdateTaskStatusRequest;
import com.sfkg.timeseries.grpc.AnomalyTaskConfig;
import com.sfkg.timeseries.grpc.QueryAnomalyResultsRequest;
import com.sfkg.timeseries.grpc.QueryAnomalyResultsResponse;
import com.sfkg.timeseries.grpc.RequestMeta;
import com.sfkg.timeseries.grpc.ResultQuery;
import com.sfkg.timeseries.grpc.TaskAck;
import com.sfkg.timeseries.grpc.TimeseriesAnalysisServiceGrpc;
import com.sfkg.timeseries.service.TimeseriesTaskContextResolver;
import com.sfkg.timeseries.vo.AnomalyResultVO;

import io.grpc.ManagedChannel;

@Component
public class AnomalyGrpcClient {

    private static final Logger LOG = LoggerFactory.getLogger(AnomalyGrpcClient.class);
    private static final String SERVICE_NAME = "timeseries-analysis";

    private final GrpcClientProperties grpcClientProperties;
    private final TimeseriesTaskContextResolver contextResolver;
    private final GrpcChannelRegistry channelRegistry;
    private final GrpcRetryExecutor retryExecutor;
    private final RetryPolicyProperties retryPolicyProperties;

    public AnomalyGrpcClient(GrpcClientProperties grpcClientProperties,
                             TimeseriesTaskContextResolver contextResolver,
                             GrpcChannelRegistry channelRegistry,
                             GrpcRetryExecutor retryExecutor,
                             RetryPolicyProperties retryPolicyProperties) {
        this.grpcClientProperties = grpcClientProperties;
        this.contextResolver = contextResolver;
        this.channelRegistry = channelRegistry;
        this.retryExecutor = retryExecutor;
        this.retryPolicyProperties = retryPolicyProperties;
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
                .setWarningRule(nullToEmpty(task.getWarningRule()))
                .setProjectId(nullToEmpty(task.getProjectId()));
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
        configBuilder.setSemanticContext(contextResolver.resolveAnomalyContext(task));

        // Task version = last config update time; P reuses its persisted model while it is unchanged.
        long taskVersion = com.sfkg.timeseries.common.ServiceTime.toEpochMillis(
                task.getUpdateTime() != null ? task.getUpdateTime() : task.getCreateTime());
        AnalysisSyncAnomalyTaskRequest req = AnalysisSyncAnomalyTaskRequest.newBuilder()
                .setMeta(newMeta(task.getProjectId()))
                .setConfigVersion(taskVersion)
                .setTaskTimestampMs(taskVersion)
                .setTask(configBuilder.build())
                .build();

        LOG.info("[{}] -> SyncAnomalyTask taskId={} version={} at {}",
                SERVICE_NAME, task.getTaskId(), taskVersion, address);
        return callSyncTask(address, req);
    }

    public SyncResult updateAnomalyTaskStatus(String taskId, String status) {
        return updateAnomalyTaskStatus(null, taskId, status);
    }

    public SyncResult updateAnomalyTaskStatus(String projectId, String taskId, String status) {
        String address = grpcClientProperties.getAnomalyAddress();
        if (isBlank(address)) {
            return notConfigured("UpdateTaskStatus");
        }

        AnalysisTaskStatus protoStatus = mapTaskStatus(status);
        AnalysisUpdateTaskStatusRequest req = AnalysisUpdateTaskStatusRequest.newBuilder()
                .setMeta(newMeta(projectId))
                .setTaskId(nullToEmpty(taskId))
                .setStatus(protoStatus)
                .setProjectId(nullToEmpty(projectId))
                .build();

        LOG.info("[{}] -> UpdateTaskStatus taskId={} status={} at {}", SERVICE_NAME, taskId, status, address);
        ManagedChannel channel = channelRegistry.getChannel(address);
        try {
            TaskAck ack = retryExecutor.execute("Analysis", "updateAnomalyTaskStatus", () ->
                    TimeseriesAnalysisServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(retryPolicyProperties.getResponseTimeoutMillis(), TimeUnit.MILLISECONDS)
                            .updateTaskStatus(req));
            LOG.info("[{}] <- UpdateTaskStatus accepted={} msg={}", SERVICE_NAME, ack.getAccepted(), ack.getMessage());
            return SyncResult.of(ack.getAccepted(), ack.getMessage());
        } catch (RuntimeException e) {
            throw e;
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
                .setLimit(10)
                .setProjectId(nullToEmpty(request.getProjectId()));

        QueryAnomalyResultsRequest req = QueryAnomalyResultsRequest.newBuilder()
                .setQuery(queryBuilder.build())
                .build();

        LOG.info("[{}] -> QueryAnomalyResults taskId={} at {}", SERVICE_NAME, request.getTaskId(), address);
        ManagedChannel channel = channelRegistry.getChannel(address);
        try {
            QueryAnomalyResultsResponse resp = retryExecutor.execute("Analysis", "queryAnomalyResults", () ->
                    TimeseriesAnalysisServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(retryPolicyProperties.getResponseTimeoutMillis(), TimeUnit.MILLISECONDS)
                            .queryAnomalyResults(req));
            LOG.info("[{}] <- QueryAnomalyResults taskId={} results={}", SERVICE_NAME,
                    resp.getTaskId(), resp.getResultsCount());
            AnomalyResultVO vo = new AnomalyResultVO();
            vo.setTaskId(resp.getTaskId());
            vo.setProjectId(request.getProjectId());
            if (resp.getResultsCount() > 0) {
                var first = resp.getResults(0);
                vo.setResultId(first.getRunId());
                vo.setStatus(first.getStatus().name());
                vo.setMessage(first.getMessage());
                if (first.getFindingsCount() > 0) {
                    var finding = first.getFindings(0);
                    vo.setAnomalyLevel(finding.getSeverity());
                    vo.setEventType(finding.getAnomalyType());
                    if (finding.hasDetectedTimeMs()) {
                        vo.setEventTime(LocalDateTime.ofInstant(
                                Instant.ofEpochMilli(finding.getDetectedTimeMs()), com.sfkg.timeseries.common.ServiceTime.ZONE_ID));
                    }
                    if (finding.getRelatedSequenceIdsCount() > 0) {
                        vo.setSequenceIds(finding.getRelatedSequenceIdsList());
                        vo.setSequenceId(finding.getRelatedSequenceIds(0));
                    }
                }
            }
            return vo;
        } catch (RuntimeException e) {
            throw e;
        }
    }

    // ── helpers ────────────────────────────────────────────────────────

    private SyncResult callSyncTask(String address, AnalysisSyncAnomalyTaskRequest req) {
        ManagedChannel channel = channelRegistry.getChannel(address);
        try {
            TaskAck ack = retryExecutor.execute("Analysis", "syncAnomalyTask", () ->
                    TimeseriesAnalysisServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(retryPolicyProperties.getResponseTimeoutMillis(), TimeUnit.MILLISECONDS)
                            .syncAnomalyTask(req));
            LOG.info("[{}] <- SyncAnomalyTask accepted={} status={} msg={}",
                    SERVICE_NAME, ack.getAccepted(), ack.getStatus(), ack.getMessage());
            return SyncResult.of(ack.getAccepted(), ack.getMessage());
        } catch (RuntimeException e) {
            throw e;
        }
    }

    private RequestMeta newMeta() {
        return newMeta(null);
    }

    private RequestMeta newMeta(String projectId) {
        return RequestMeta.newBuilder()
                .setRequestId(UUID.randomUUID().toString())
                .setSentAtMs(System.currentTimeMillis())
                .setProjectId(nullToEmpty(projectId))
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
