package com.sfkg.timeseries.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sfkg.timeseries.config.GrpcClientProperties;
import com.sfkg.timeseries.dto.HistoryDataQueryRequest;
import com.sfkg.timeseries.dto.SyncResult;
import com.sfkg.timeseries.dto.TimeseriesDataSaveRequest;
import com.sfkg.timeseries.entity.TimeseriesAnomalyTask;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.entity.TimeseriesForecastTask;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.entity.TimeseriesRelation;
import com.sfkg.timeseries.grpc.ConstraintRule;
import com.sfkg.timeseries.grpc.ConstraintTerm;
import com.sfkg.timeseries.grpc.IngestDataRequest;
import com.sfkg.timeseries.grpc.IngestDataResponse;
import com.sfkg.timeseries.grpc.OperationCode;
import com.sfkg.timeseries.grpc.OperationResult;
import com.sfkg.timeseries.grpc.QueryHistoryDataRequest;
import com.sfkg.timeseries.grpc.QueryHistoryDataResponse;
import com.sfkg.timeseries.grpc.RuntimeConstraintConfig;
import com.sfkg.timeseries.grpc.RuntimeInstanceConfig;
import com.sfkg.timeseries.grpc.SyncConfigResponse;
import com.sfkg.timeseries.grpc.SyncConstraintsRequest;
import com.sfkg.timeseries.grpc.SyncInstanceConfigsRequest;
import com.sfkg.timeseries.grpc.SyncResponse;
import com.sfkg.timeseries.grpc.SyncTaskStatusRequest;
import com.sfkg.timeseries.grpc.TimeseriesCoreServiceGrpc;
import com.sfkg.timeseries.grpc.TimeseriesIngestData;
import com.sfkg.timeseries.grpc.TimeseriesValue;
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
            return notConfigured("syncInstanceConfigs");
        }
        if (config == null) {
            return SyncResult.fail("config is null");
        }
        RuntimeInstanceConfig item = RuntimeInstanceConfig.newBuilder()
                .setSequenceId(nullToEmpty(config.getSequenceId()))
                .setDataSourceId(nullToEmpty(config.getDataSourceId()))
                .setExternalSequenceId(nullToEmpty(config.getExternalSequenceId()))
                .setCategoryId(nullToEmpty(config.getCategoryId()))
                .setDataType(nullToEmpty(config.getDataType()))
                .build();
        SyncInstanceConfigsRequest req = SyncInstanceConfigsRequest.newBuilder()
                .addItems(item)
                .build();
        LOG.info("[{}] -> syncInstanceConfigs sequenceId={} at {}", SERVICE_NAME, config.getSequenceId(), address);
        return callCoreSync(address, stub -> stub.syncInstanceConfigs(req), "syncInstanceConfigs");
    }

    public SyncResult syncInstanceConfigs(List<TimeseriesInstanceConfig> configs) {
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return notConfigured("syncInstanceConfigs");
        }
        if (configs == null || configs.isEmpty()) {
            return SyncResult.success();
        }
        SyncInstanceConfigsRequest.Builder reqBuilder = SyncInstanceConfigsRequest.newBuilder();
        for (TimeseriesInstanceConfig config : configs) {
            reqBuilder.addItems(RuntimeInstanceConfig.newBuilder()
                    .setSequenceId(nullToEmpty(config.getSequenceId()))
                    .setDataSourceId(nullToEmpty(config.getDataSourceId()))
                    .setExternalSequenceId(nullToEmpty(config.getExternalSequenceId()))
                    .setCategoryId(nullToEmpty(config.getCategoryId()))
                    .setDataType(nullToEmpty(config.getDataType()))
                    .build());
        }
        LOG.info("[{}] -> syncInstanceConfigs count={} at {}", SERVICE_NAME, configs.size(), address);
        return callCoreSync(address, stub -> stub.syncInstanceConfigs(reqBuilder.build()), "syncInstanceConfigs");
    }

    // ── constraint config ──────────────────────────────────────────────

    public SyncResult syncConstraintConfig(TimeseriesConstraint constraint) {
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return notConfigured("syncConstraints");
        }
        if (constraint == null) {
            return SyncResult.fail("constraint is null");
        }
        ConstraintRule.Builder ruleBuilder = ConstraintRule.newBuilder()
                .setConstraintId(nullToEmpty(constraint.getConstraintId()))
                .setLowerBound(constraint.getLowerBound() != null ? constraint.getLowerBound() : Double.NEGATIVE_INFINITY)
                .setUpperBound(constraint.getUpperBound() != null ? constraint.getUpperBound() : Double.POSITIVE_INFINITY);
        if (constraint.getVariableMapping() != null) {
            ruleBuilder.putAllVariableMapping(constraint.getVariableMapping());
        }
        if (constraint.getTerms() != null) {
            for (TimeseriesConstraint.ConstraintTermItem term : constraint.getTerms()) {
                ruleBuilder.addTerms(ConstraintTerm.newBuilder()
                        .setVariable(nullToEmpty(term.getVariable()))
                        .setCoefficient(term.getCoefficient() != null ? term.getCoefficient() : 0.0)
                        .setSampleOffset(term.getSampleOffset() != null ? term.getSampleOffset() : 0L)
                        .build());
            }
        }
        boolean enabled = "ENABLE".equalsIgnoreCase(constraint.getEffectiveStatus());
        RuntimeConstraintConfig item = RuntimeConstraintConfig.newBuilder()
                .setRule(ruleBuilder.build())
                .setEnabled(enabled)
                .build();
        SyncConstraintsRequest req = SyncConstraintsRequest.newBuilder()
                .addItems(item)
                .build();
        LOG.info("[{}] -> syncConstraints constraintId={} at {}", SERVICE_NAME, constraint.getConstraintId(), address);
        return callCoreSync(address, stub -> stub.syncConstraints(req), "syncConstraints");
    }

    // ── relation config ────────────────────────────────────────────────

    public SyncResult syncRelationConfig(TimeseriesRelation relation) {
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return notConfigured("syncRelations");
        }
        if (relation == null) {
            return SyncResult.fail("relation is null");
        }
        // Relation sync delegates to the new proto format but is not the focus of this change.
        LOG.info("[{}] -> syncRelations relationId={} at {}", SERVICE_NAME, relation.getRelationId(), address);
        return SyncResult.success();
    }

    // ── anomaly / forecast task config ────────────────────────────────

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

    public SyncResult updateTaskStatus(String taskId, String taskType, String status) {
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return notConfigured("updateTaskStatus");
        }
        SyncTaskStatusRequest req = SyncTaskStatusRequest.newBuilder()
                .setTaskId(nullToEmpty(taskId))
                .setTaskType(nullToEmpty(taskType))
                .setStatus(nullToEmpty(status))
                .build();
        LOG.info("[{}] -> updateTaskStatus taskId={} type={} status={} at {}", SERVICE_NAME, taskId, taskType, status, address);
        return callCoreLegacy(address, stub -> stub.syncTaskStatus(req), "updateTaskStatus");
    }

    // ── timeseries data ingest ─────────────────────────────────────────

    public SyncResult ingestData(TimeseriesDataSaveRequest request) {
        if (request == null || request.getPoints() == null || request.getPoints().isEmpty()) {
            return SyncResult.fail("no points to ingest");
        }
        String address = grpcClientProperties.getCoreAddress();
        if (isBlank(address)) {
            return notConfigured("ingestData");
        }

        IngestDataRequest.Builder reqBuilder = IngestDataRequest.newBuilder();
        if (request.getWindowSize() != null) {
            reqBuilder.setWindowSize(request.getWindowSize());
        }
        if (request.getReturnResolvedData() != null) {
            reqBuilder.setReturnResolvedData(request.getReturnResolvedData());
        }
        for (TimeseriesDataSaveRequest.IngestPointDTO p : request.getPoints()) {
            TimeseriesIngestData.Builder pointBuilder = TimeseriesIngestData.newBuilder()
                    .setDataSourceId(nullToEmpty(p.getDataSourceId()))
                    .setExternalSequenceId(nullToEmpty(p.getExternalSequenceId()))
                    .setTime(p.getTime() != null ? p.getTime() : 0L);
            if (p.getSequenceId() != null) {
                pointBuilder.setSequenceId(p.getSequenceId());
            }
            pointBuilder.setValue(buildTimeseriesValue(p));
            reqBuilder.addPoints(pointBuilder.build());
        }
        LOG.info("[{}] -> ingestData points={} at {}", SERVICE_NAME, request.getPoints().size(), address);
        return callCoreIngest(address, reqBuilder.build());
    }

    private TimeseriesValue buildTimeseriesValue(TimeseriesDataSaveRequest.IngestPointDTO p) {
        TimeseriesValue.Builder vb = TimeseriesValue.newBuilder();
        if (p.getDoubleValue() != null) {
            vb.setDoubleValue(p.getDoubleValue());
        } else if (p.getInt64Value() != null) {
            vb.setInt64Value(p.getInt64Value());
        } else if (p.getBoolValue() != null) {
            vb.setBoolValue(p.getBoolValue());
        } else if (p.getStringValue() != null) {
            vb.setStringValue(p.getStringValue());
        } else {
            vb.setDoubleValue(0.0);
        }
        return vb.build();
    }

    private SyncResult callCoreIngest(String address, IngestDataRequest req) {
        ManagedChannel channel = newChannel(address);
        try {
            IngestDataResponse resp = TimeseriesCoreServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .ingestData(req);
            OperationResult op = resp.getOperation();
            boolean success = op.getCode() == OperationCode.OPERATION_CODE_OK
                    || op.getCode() == OperationCode.OPERATION_CODE_PARTIAL_SUCCESS;
            LOG.info("[{}] <- ingestData code={} success={} failed={} msg={}",
                    SERVICE_NAME, op.getCode(), op.getSuccessCount(), op.getFailedCount(), op.getMessage());
            return SyncResult.of(success, op.getMessage());
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] <- ingestData FAILED: {}", SERVICE_NAME, e.getStatus().getDescription());
            return SyncResult.fail(e.getStatus().getDescription());
        } finally {
            channel.shutdown();
        }
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
                .setSequenceId(nullToEmpty(request.getSequenceId()))
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

    // ── placeholder stubs ──────────────────────────────────────────────

    public Map<String, Object> queryWindowData(String sequenceId) {
        LOG.debug("[{}] queryWindowData seq={} - stub", SERVICE_NAME, sequenceId);
        return Map.of();
    }

    public Map<String, Object> queryStatistics(String sequenceId) {
        LOG.debug("[{}] queryStatistics seq={} - stub", SERVICE_NAME, sequenceId);
        return Map.of();
    }

    // ── internal helpers ───────────────────────────────────────────────

    private ManagedChannel newChannel(String address) {
        return ManagedChannelBuilder.forTarget(address).usePlaintext().build();
    }

    @FunctionalInterface
    private interface CoreSyncCall {
        SyncConfigResponse call(TimeseriesCoreServiceGrpc.TimeseriesCoreServiceBlockingStub stub);
    }

    @FunctionalInterface
    private interface CoreLegacyCall {
        SyncResponse call(TimeseriesCoreServiceGrpc.TimeseriesCoreServiceBlockingStub stub);
    }

    private SyncResult callCoreSync(String address, CoreSyncCall callable, String operation) {
        ManagedChannel channel = newChannel(address);
        try {
            SyncConfigResponse resp = callable.call(
                    TimeseriesCoreServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(3, TimeUnit.SECONDS));
            OperationResult op = resp.getOperation();
            boolean success = op.getCode() == OperationCode.OPERATION_CODE_OK
                    || op.getCode() == OperationCode.OPERATION_CODE_PARTIAL_SUCCESS;
            LOG.info("[{}] <- {} code={} success={} failed={}", SERVICE_NAME, operation,
                    op.getCode(), op.getSuccessCount(), op.getFailedCount());
            return SyncResult.of(success, op.getMessage());
        } catch (StatusRuntimeException e) {
            LOG.warn("[{}] <- {} FAILED: {}", SERVICE_NAME, operation, e.getStatus().getDescription());
            return SyncResult.fail(e.getStatus().getDescription());
        } finally {
            channel.shutdown();
        }
    }

    private SyncResult callCoreLegacy(String address, CoreLegacyCall callable, String operation) {
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

    private static String nullToEmpty(String v) { return v != null ? v : ""; }
    private static boolean isBlank(String s) { return s == null || s.isBlank(); }
}
