package com.sfkg.timeseries.grpc.server;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.protobuf.Empty;
import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
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

            cacheManager.ensureTableLoaded(CachedTable.ANOMALY_RESULT);

            TimeseriesAnomalyResult entity = toEntity(request);
            anomalyResultMapper.insert(entity);
            memoryCache.putAnomalyResult(entity);

            // create an event from this anomaly result
            AnomalyResultVO vo = new AnomalyResultVO();
            vo.setResultId(entity.getResultId());
            vo.setTaskId(entity.getTaskId());
            vo.setSequenceId(entity.getSequenceIds() != null && !entity.getSequenceIds().isEmpty()
                    ? String.join(",", entity.getSequenceIds()) : null);
            vo.setAnomalyLevel(entity.getSeverity());
            vo.setEventType(entity.getEventType());
            vo.setEventTime(entity.getEventTime());
            vo.setSource(entity.getSource());
            vo.setValues(entity.getValues());
            anomalyResultService.createAnomalyEvent(vo);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            LOG.info("gRPC receiveAnomalyResult success: resultId={}", entity.getResultId());
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

            cacheManager.ensureTableLoaded(CachedTable.FORECAST_RESULT);

            TimeseriesForecastResult entity = toForecastEntity(request);
            forecastResultMapper.insert(entity);
            memoryCache.putForecastResult(entity);

            // create a forecast warning event (idempotent via computeEvent)
            String ts = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyMMddHHmmssSSS"));
            String taskId = entity.getTaskId() != null ? entity.getTaskId() : "UNKNOWN";
            String eventId = "EVT_FORECAST_" + taskId + "_" + ts;
            memoryCache.computeEvent(eventId, existing -> {
                if (existing != null) {
                    LOG.info("forecast event already exists, skip: eventId={}", eventId);
                    return existing;
                }
                TimeseriesEvent event = new TimeseriesEvent();
                event.setEventId(eventId);
                event.setEventName("forecast event on " + taskId);
                event.setEventType("WARNING");
                event.setEventSource("FORECAST");
                event.setTaskId(taskId);
                event.setEventLevel("MEDIUM");
                event.setEventTime(LocalDateTime.now());
                event.setRelatedSequences(entity.getSequenceIds());
                event.setConfirmStatus("PENDING");
                event.setHandleStatus("UNHANDLED");
                LocalDateTime now = LocalDateTime.now();
                event.setCreateTime(now);
                event.setUpdateTime(now);
                eventMapper.insert(event);
                return event;
            });

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            LOG.info("gRPC receiveForecastResult success: resultId={}", entity.getResultId());
        } catch (Exception e) {
            LOG.error("gRPC receiveForecastResult failed", e);
            responseObserver.onError(e);
        }
    }

    private TimeseriesAnomalyResult toEntity(AnomalyResultMessage msg) {
        TimeseriesAnomalyResult entity = new TimeseriesAnomalyResult();
        String source = msg.getSource().name();
        entity.setTaskId(emptyToNull(msg.getTaskId()));
        String seqs = String.join("_", msg.getSequenceIdsList());
        entity.setResultId(source + "_" + seqs + "_" + msg.getEventTimeMs());
        entity.setEventType(msg.getEventType().name());
        entity.setEventTime(msg.getEventTimeMs() > 0
                ? LocalDateTime.ofInstant(Instant.ofEpochMilli(msg.getEventTimeMs()), ZoneId.systemDefault())
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
        entity.setResultId("FR_" + msg.getTaskId() + "_" + msg.getRunId() + "_" + UUID.randomUUID().toString().substring(0, 8));
        entity.setTaskId(emptyToNull(msg.getTaskId()));
        entity.setRunId(emptyToNull(msg.getRunId()));
        entity.setGeneratedAt(msg.getGeneratedAtMs() > 0
                ? LocalDateTime.ofInstant(Instant.ofEpochMilli(msg.getGeneratedAtMs()), ZoneId.systemDefault())
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
}
