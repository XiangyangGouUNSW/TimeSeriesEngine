package com.sfkg.timeseries.grpc.server;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.protobuf.Empty;
import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.common.ProjectIdValidator;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.entity.TimeseriesAnomalyResult;
import com.sfkg.timeseries.entity.TimeseriesEvent;
import com.sfkg.timeseries.entity.TimeseriesForecastResult;
import com.sfkg.timeseries.grpc.AnalysisResultReceiverServiceGrpc;
import com.sfkg.timeseries.grpc.AnalysisStatus;
import com.sfkg.timeseries.grpc.AnomalyResultMessage;
import com.sfkg.timeseries.grpc.ForecastResultMessage;
import com.sfkg.timeseries.mapper.TimeseriesAnomalyResultMapper;
import com.sfkg.timeseries.mapper.TimeseriesEventMapper;
import com.sfkg.timeseries.mapper.TimeseriesForecastResultMapper;
import com.sfkg.timeseries.service.TimeseriesAnomalyResultService;
import com.sfkg.timeseries.vo.AnomalyResultVO;

import io.grpc.stub.StreamObserver;
import io.grpc.Status;

@Component
public class AnomalyResultReceiverGrpcService
        extends AnalysisResultReceiverServiceGrpc.AnalysisResultReceiverServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(AnomalyResultReceiverGrpcService.class);

    private final TimeseriesAnomalyResultMapper anomalyResultMapper;
    private final TimeseriesForecastResultMapper forecastResultMapper;
    private final TimeseriesEventMapper eventMapper;
    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesCacheManager cacheManager;
    private final TimeseriesAnomalyResultService anomalyResultService;

    public AnomalyResultReceiverGrpcService(
            TimeseriesAnomalyResultMapper anomalyResultMapper,
            TimeseriesForecastResultMapper forecastResultMapper,
            TimeseriesEventMapper eventMapper,
            TimeseriesMemoryCache memoryCache,
            TimeseriesCacheManager cacheManager,
            TimeseriesAnomalyResultService anomalyResultService) {
        this.anomalyResultMapper = anomalyResultMapper;
        this.forecastResultMapper = forecastResultMapper;
        this.eventMapper = eventMapper;
        this.memoryCache = memoryCache;
        this.cacheManager = cacheManager;
        this.anomalyResultService = anomalyResultService;
    }

    @Override
    public void receiveAnomalyResult(AnomalyResultMessage request,
                                     StreamObserver<Empty> responseObserver) {
        try {
            LOG.info("gRPC receiveAnomalyResult: taskId={}, eventType={}, severity={}, source={}, seqs={}",
                    request.getTaskId(), request.getEventType(), request.getSeverity(), request.getSource(),
                    request.getSequenceIdsList());

            validateAnomalyResult(request);
            cacheManager.ensureTableLoaded(CachedTable.ANOMALY_RESULT);

            TimeseriesAnomalyResult entity = toEntity(request);
            boolean duplicate = memoryCache.getAnomalyResult(entity.getProjectId(), entity.getResultId())
                    .isPresent();
            anomalyResultMapper.insert(entity);
            memoryCache.putAnomalyResult(entity);

            if (!duplicate) {
                // Create an event only for a newly received business result.
                AnomalyResultVO vo = new AnomalyResultVO();
                vo.setProjectId(entity.getProjectId());
                vo.setResultId(entity.getResultId());
                vo.setTaskId(entity.getTaskId());
                vo.setSequenceIds(entity.getSequenceIds());
                vo.setSequenceId(entity.getSequenceIds() != null && !entity.getSequenceIds().isEmpty()
                        ? String.join(",", entity.getSequenceIds()) : null);
                vo.setAnomalyLevel(entity.getSeverity());
                vo.setEventType(entity.getEventType());
                vo.setEventTime(entity.getEventTime());
                vo.setSource(entity.getSource());
                vo.setValues(entity.getValues());
                if (entity.getTaskId() != null) {
                    cacheManager.ensureTableLoaded(CachedTable.ANOMALY_TASK);
                    memoryCache.getAnomalyTask(entity.getProjectId(), entity.getTaskId())
                            .ifPresent(task -> vo.setConstraintIds(task.getConstraintIds()));
                }
                anomalyResultService.createAnomalyEvent(vo);
            }

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            LOG.info("gRPC receiveAnomalyResult success: resultId={}", entity.getResultId());
        } catch (IllegalArgumentException | BusinessException e) {
            LOG.warn("gRPC receiveAnomalyResult rejected invalid input: {}", e.getMessage());
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            LOG.error("gRPC receiveAnomalyResult failed", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void receiveForecastResult(ForecastResultMessage request,
                                      StreamObserver<Empty> responseObserver) {
        try {
            LOG.info("gRPC receiveForecastResult: taskId={}, runId={}, status={}, seqs={}",
                    request.getTaskId(), request.getRunId(), request.getStatus(),
                    request.getSequenceIdsList());

            validateForecastResult(request);
            cacheManager.ensureTableLoaded(CachedTable.FORECAST_RESULT);

            TimeseriesForecastResult entity = toForecastEntity(request);
            boolean duplicate = memoryCache.getForecastResult(entity.getProjectId(), entity.getResultId())
                    .isPresent();
            forecastResultMapper.insert(entity);
            memoryCache.putForecastResult(entity);

            if (!duplicate) {
                cacheManager.ensureTableLoaded(CachedTable.EVENT);
                String taskId = entity.getTaskId();
                String eventId = "EVT_FORECAST_" + taskId + "_" + entity.getRunId();
                memoryCache.computeEvent(entity.getProjectId(), eventId, existing -> {
                    if (existing != null) {
                        return existing;
                    }
                    TimeseriesEvent event = new TimeseriesEvent();
                    event.setProjectId(entity.getProjectId());
                    event.setEventId(eventId);
                    event.setEventName("forecast event on " + taskId);
                    event.setEventType("WARNING");
                    event.setEventSource("FORECAST");
                    event.setTaskId(taskId);
                    event.setEventLevel("MEDIUM");
                    event.setEventTime(entity.getGeneratedAt());
                    event.setRelatedSequences(entity.getSequenceIds());
                    event.setConfirmStatus("PENDING");
                    event.setHandleStatus("UNHANDLED");
                    LocalDateTime now = LocalDateTime.now();
                    event.setCreateTime(now);
                    event.setUpdateTime(now);
                    eventMapper.insert(event);
                    return event;
                });
            }

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            LOG.info("gRPC receiveForecastResult success: resultId={}", entity.getResultId());
        } catch (IllegalArgumentException | BusinessException e) {
            LOG.warn("gRPC receiveForecastResult rejected invalid input: {}", e.getMessage());
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            LOG.error("gRPC receiveForecastResult failed", e);
            responseObserver.onError(e);
        }
    }

    private TimeseriesAnomalyResult toEntity(AnomalyResultMessage msg) {
        TimeseriesAnomalyResult entity = new TimeseriesAnomalyResult();
        entity.setProjectId(emptyToNull(msg.getProjectId()));
        String source = msg.getSource().name();
        entity.setTaskId(emptyToNull(msg.getTaskId()));
        String key = emptyToNull(msg.getProjectId()) + "|" + emptyToNull(msg.getTaskId()) + "|"
                + source + "|" + canonicalIds(msg.getSequenceIdsList()) + "|" + msg.getEventTimeMs();
        entity.setResultId("AR_" + UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8)));
        entity.setEventType(msg.getEventType().name());
        entity.setEventTime(msg.getEventTimeMs() > 0
                ? LocalDateTime.ofInstant(Instant.ofEpochMilli(msg.getEventTimeMs()), com.sfkg.timeseries.common.ServiceTime.ZONE_ID)
                : null);
        entity.setSequenceIds(msg.getSequenceIdsCount() > 0
                ? List.copyOf(msg.getSequenceIdsList()) : null);
        entity.setValues(msg.getValuesCount() > 0
                ? new java.util.ArrayList<>(msg.getValuesList()) : null);
        entity.setSeverity(msg.getSeverity().name());
        entity.setSource(source);
        entity.setReceivedTime(LocalDateTime.now());
        return entity;
    }

    private TimeseriesForecastResult toForecastEntity(ForecastResultMessage msg) {
        TimeseriesForecastResult entity = new TimeseriesForecastResult();
        entity.setProjectId(emptyToNull(msg.getProjectId()));
        entity.setResultId("FR_" + msg.getTaskId() + "_" + msg.getRunId());
        entity.setTaskId(emptyToNull(msg.getTaskId()));
        entity.setRunId(emptyToNull(msg.getRunId()));
        entity.setGeneratedAt(msg.getGeneratedAtMs() > 0
                ? LocalDateTime.ofInstant(Instant.ofEpochMilli(msg.getGeneratedAtMs()), com.sfkg.timeseries.common.ServiceTime.ZONE_ID)
                : null);
        entity.setStatus(msg.getStatus() != null && msg.getStatus() != AnalysisStatus.ANALYSIS_STATUS_UNSPECIFIED
                ? msg.getStatus().name() : null);
        entity.setMessage(emptyToNull(msg.getMessage()));
        entity.setTimestampsMs(msg.getTimestampsMsCount() > 0
                ? new ArrayList<>(msg.getTimestampsMsList()) : null);
        entity.setSequenceIds(msg.getSequenceIdsCount() > 0
                ? List.copyOf(msg.getSequenceIdsList()) : null);
        entity.setValues(msg.getValuesCount() > 0
                ? new ArrayList<>(msg.getValuesList()) : null);
        entity.setReceivedTime(LocalDateTime.now());
        return entity;
    }

    private String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private String canonicalIds(List<String> values) {
        return values.stream()
                .sorted()
                .map(value -> value.length() + ":" + value)
                .collect(Collectors.joining("|"));
    }

    private void validateAnomalyResult(AnomalyResultMessage request) {
        ProjectIdValidator.require(request.getProjectId());
        if (request.getEventTimeMs() <= 0) {
            throw new IllegalArgumentException("anomaly result eventTimeMs must be positive");
        }
        if (request.getSequenceIdsCount() == 0) {
            throw new IllegalArgumentException("anomaly result sequenceIds must not be empty");
        }
    }

    private void validateForecastResult(ForecastResultMessage request) {
        ProjectIdValidator.require(request.getProjectId());
        if (request.getTaskId().isBlank() || request.getRunId().isBlank() || request.getGeneratedAtMs() <= 0) {
            throw new IllegalArgumentException("forecast result requires taskId, runId, and positive generatedAtMs");
        }
        if (request.getTimestampsMsCount() != request.getValuesCount()) {
            throw new IllegalArgumentException("forecast timestampsMs and values must have equal lengths");
        }
        long previous = 0;
        for (long timestamp : request.getTimestampsMsList()) {
            if (timestamp <= 0 || timestamp <= previous) {
                throw new IllegalArgumentException("forecast timestampsMs must be positive and strictly increasing");
            }
            previous = timestamp;
        }
    }
}
