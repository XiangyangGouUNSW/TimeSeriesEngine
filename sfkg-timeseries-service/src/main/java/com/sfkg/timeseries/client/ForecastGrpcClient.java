package com.sfkg.timeseries.client;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.sfkg.timeseries.config.GrpcClientProperties;
import com.sfkg.timeseries.config.RetryPolicyProperties;
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
import com.sfkg.timeseries.service.TimeseriesTaskContextResolver;
import com.sfkg.timeseries.vo.ForecastResultVO;

import io.grpc.ManagedChannel;

@Component
public class ForecastGrpcClient {

    private static final Logger LOG = LoggerFactory.getLogger(ForecastGrpcClient.class);
    private static final String SERVICE_NAME = "timeseries-analysis";

    private final GrpcClientProperties grpcClientProperties;
    private final TimeseriesTaskContextResolver contextResolver;
    private final GrpcChannelRegistry channelRegistry;
    private final GrpcRetryExecutor retryExecutor;
    private final RetryPolicyProperties retryPolicyProperties;

    public ForecastGrpcClient(GrpcClientProperties grpcClientProperties,
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
                .setTaskName(nullToEmpty(task.getTaskName()))
                .setProjectId(nullToEmpty(task.getProjectId()));
        if (task.getForecastObjects() != null) {
            configBuilder.addAllTargetSequenceIds(task.getForecastObjects());
        }
        if (task.getForecastHorizon() != null) {
            try {
                configBuilder.setForecastHorizonSteps(Integer.parseInt(task.getForecastHorizon()));
            } catch (NumberFormatException ignored) {}
        }
        // feature_sequence_ids: original + auto-discovered from relations (category-level expanded)
        configBuilder.addAllFeatureSequenceIds(contextResolver.resolveForecastFeatureIds(task));
        if (task.getObservationWindowMs() != null) {
            configBuilder.setObservationWindowMs(task.getObservationWindowMs());
        }
        if (task.getMinimumPoints() != null) {
            configBuilder.setMinimumPoints(task.getMinimumPoints());
        }
        if (task.getModelKey() != null) {
            configBuilder.setModelKey(task.getModelKey());
        }
        configBuilder.setSemanticContext(contextResolver.resolveForecastContext(task));

        long taskVersion = com.sfkg.timeseries.common.ServiceTime.toEpochMillis(
                task.getUpdateTime() != null ? task.getUpdateTime() : task.getCreateTime());
        AnalysisSyncForecastTaskRequest req = AnalysisSyncForecastTaskRequest.newBuilder()
                .setMeta(newMeta(task.getProjectId()))
                .setConfigVersion(taskVersion)
                .setTaskTimestampMs(taskVersion)
                .setTask(configBuilder.build())
                .build();

        LOG.info("[{}] -> SyncForecastTask taskId={} version={} at {}",
                SERVICE_NAME, task.getTaskId(), taskVersion, address);
        ManagedChannel channel = channelRegistry.getChannel(address);
        try {
            TaskAck ack = retryExecutor.execute("Analysis", "syncForecastTask", () ->
                    TimeseriesAnalysisServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(retryPolicyProperties.getResponseTimeoutMillis(), TimeUnit.MILLISECONDS)
                            .syncForecastTask(req));
            LOG.info("[{}] <- SyncForecastTask accepted={} msg={}", SERVICE_NAME, ack.getAccepted(), ack.getMessage());
            return SyncResult.of(ack.getAccepted(), ack.getMessage());
        } catch (RuntimeException e) {
            throw e;
        }
    }

    public SyncResult updateForecastTaskStatus(String taskId, String status) {
        return updateForecastTaskStatus(null, taskId, status);
    }

    public SyncResult updateForecastTaskStatus(String projectId, String taskId, String status) {
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
                .setMeta(newMeta(projectId))
                .setTaskId(nullToEmpty(taskId))
                .setStatus(protoStatus)
                .setProjectId(nullToEmpty(projectId))
                .build();
        LOG.info("[{}] -> UpdateTaskStatus taskId={} status={} at {}", SERVICE_NAME, taskId, status, address);
        ManagedChannel channel = channelRegistry.getChannel(address);
        try {
            TaskAck ack = retryExecutor.execute("Analysis", "updateForecastTaskStatus", () ->
                    TimeseriesAnalysisServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(retryPolicyProperties.getResponseTimeoutMillis(), TimeUnit.MILLISECONDS)
                            .updateTaskStatus(req));
            return SyncResult.of(ack.getAccepted(), ack.getMessage());
        } catch (RuntimeException e) {
            throw e;
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
                        .setProjectId(nullToEmpty(request.getProjectId()))
                        .build())
                .build();
        LOG.info("[{}] -> QueryForecastResults taskId={} at {}", SERVICE_NAME, request.getTaskId(), address);
        ManagedChannel channel = channelRegistry.getChannel(address);
        try {
            QueryForecastResultsResponse resp = retryExecutor.execute("Analysis", "queryForecastResults", () ->
                    TimeseriesAnalysisServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(retryPolicyProperties.getResponseTimeoutMillis(), TimeUnit.MILLISECONDS)
                            .queryForecastResults(req));
            ForecastResultVO vo = new ForecastResultVO();
            vo.setTaskId(resp.getTaskId());
            vo.setProjectId(request.getProjectId());
            if (resp.getResultsCount() > 0) {
                var result = resp.getResults(0);
                vo.setResultId(result.getRunId());
                vo.setStatus(result.getStatus().name());
                vo.setMessage(result.getMessage());
                vo.setSequenceIds(List.copyOf(result.getSequenceIdsList()));
                if (result.getSequenceIdsCount() > 0) {
                    vo.setSequenceId(result.getSequenceIds(0));
                }
                vo.setValues(List.copyOf(result.getValuesList()));
                if (result.getRiskFindingsCount() > 0) {
                    vo.setWarningLevel(result.getRiskFindings(0).getSeverity());
                }
            }
            return vo;
        } catch (RuntimeException e) {
            throw e;
        }
    }

    // ── helpers ────────────────────────────────────────────────────────

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

    private SyncResult notConfigured(String operation) {
        LOG.info("[{}] {} skipped: forecast address not configured", SERVICE_NAME, operation);
        return SyncResult.fail("forecast address not configured");
    }

    private static String nullToEmpty(String v) { return v != null ? v : ""; }
    private static boolean isBlank(String s) { return s == null || s.isBlank(); }
}
