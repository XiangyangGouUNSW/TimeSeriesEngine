package com.sfkg.timeseries.service.impl;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.client.AnomalyGrpcClient;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.common.ProjectIdValidator;
import com.sfkg.timeseries.dto.AnomalyResultQueryRequest;
import com.sfkg.timeseries.entity.TimeseriesEvent;
import com.sfkg.timeseries.mapper.TimeseriesEventMapper;
import com.sfkg.timeseries.service.TimeseriesAnomalyResultService;
import com.sfkg.timeseries.vo.AnomalyResultVO;

@Service
public class TimeseriesAnomalyResultServiceImpl implements TimeseriesAnomalyResultService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeseriesAnomalyResultServiceImpl.class);

    private final TimeseriesEventMapper eventMapper;
    private final TimeseriesMemoryCache memoryCache;
    private final AnomalyGrpcClient anomalyGrpcClient;
    private final TimeseriesCacheManager cacheManager;

    public TimeseriesAnomalyResultServiceImpl(
            TimeseriesEventMapper eventMapper,
            TimeseriesMemoryCache memoryCache,
            AnomalyGrpcClient anomalyGrpcClient,
            TimeseriesCacheManager cacheManager) {
        this.eventMapper = eventMapper;
        this.memoryCache = memoryCache;
        this.anomalyGrpcClient = anomalyGrpcClient;
        this.cacheManager = cacheManager;
    }

    @Override
    public AnomalyResultVO queryAnomalyResults(AnomalyResultQueryRequest request) {
        validateQuery(request);
        return anomalyGrpcClient.queryAnomalyResult(request);
    }

    @Override
    public AnomalyResultVO handleAnomalyResult(Object rawResult) {
        return new AnomalyResultVO();
    }

    @Override
    public String createAnomalyEvent(AnomalyResultVO result) {
        String source = result != null && result.getSource() != null ? result.getSource() : "ANOMALY";
        List<String> seqIds = result != null && result.getSequenceIds() != null
                ? result.getSequenceIds()
                : (result != null && result.getSequenceId() != null
                    ? List.of(result.getSequenceId())
                    : List.of());
        String seq = seqIds.isEmpty() ? "UNKNOWN" : String.join("_", seqIds);
        String ts = java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyMMddHHmmssSSS"));
        String eventId = "EVT_" + source + "_" + seq + "_" + ts;
        TimeseriesEvent event = new TimeseriesEvent();
        event.setProjectId(result != null ? result.getProjectId() : null);
        event.setEventId(eventId);
        event.setEventName(source + " event on " + seq);
        event.setEventType(stripAnomalyPrefix(result == null ? null : result.getEventType(),
                "ANOMALY"));
        event.setEventSource(result != null && result.getSource() != null
                ? result.getSource() : "ANOMALY_DETECTION");
        event.setTaskId(result != null ? result.getTaskId() : null);
        event.setEventLevel(stripSeverityPrefix(result == null ? null : result.getAnomalyLevel()));
        event.setRelatedSequences(seqIds);
        event.setRelatedRules(result != null ? result.getConstraintIds() : null);
        event.setEventTime(result != null && result.getEventTime() != null
                ? result.getEventTime() : LocalDateTime.now());
        event.setEventDescription(result != null
                ? buildAnomalyDescription(result) : null);
        event.setConfirmStatus("PENDING");
        event.setHandleStatus("UNHANDLED");
        // audit fields
        LocalDateTime now = LocalDateTime.now();
        event.setCreateTime(now);
        event.setUpdateTime(now);

        // idempotent: skip if event already exists
        memoryCache.computeEvent(event.getProjectId(), eventId, existing -> {
            if (existing != null) {
                LOGGER.info("anomaly event already exists, skip: eventId={}", eventId);
                return existing;
            }
            eventMapper.insert(event);
            return event;
        });
        return eventId;
    }

    private String stripSeverityPrefix(String raw) {
        if (raw == null) return null;
        return raw.startsWith("SEVERITY_") ? raw.substring("SEVERITY_".length()) : raw;
    }

    private String stripAnomalyPrefix(String raw, String fallback) {
        if (raw == null) return fallback;
        if (raw.startsWith("ANOMALY_EVENT_TYPE_")) return raw.substring("ANOMALY_EVENT_TYPE_".length());
        return raw;
    }

    private String buildAnomalyDescription(AnomalyResultVO result) {
        StringBuilder sb = new StringBuilder();
        sb.append("Anomaly detected");
        if (result.getSequenceIds() != null && !result.getSequenceIds().isEmpty()) {
            sb.append(" on sequences ").append(String.join(", ", result.getSequenceIds()));
        } else if (result.getSequenceId() != null) {
            sb.append(" on sequence ").append(result.getSequenceId());
        }
        if (result.getAnomalyLevel() != null) {
            sb.append(" with severity ").append(stripSeverityPrefix(result.getAnomalyLevel()));
        }
        if (result.getValues() != null && !result.getValues().isEmpty()) {
            sb.append(", values: ").append(result.getValues());
        }
        if (result.getConstraintIds() != null && !result.getConstraintIds().isEmpty()) {
            sb.append(", triggered constraints: ").append(String.join(", ", result.getConstraintIds()));
        }
        return sb.toString();
    }

    private void validateQuery(AnomalyResultQueryRequest request) {
        if (request == null) {
            throw new BusinessException("anomaly result query must not be null");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        if (request.getTaskId() == null || request.getTaskId().isBlank()
                || request.getSequenceId() == null || request.getSequenceId().isBlank()) {
            throw new BusinessException("anomaly result query requires taskId and sequenceId");
        }
        if (request.getStartTime() != null && request.getEndTime() != null
                && request.getStartTime().isAfter(request.getEndTime())) {
            throw new BusinessException("anomaly result query startTime must be before endTime");
        }
        if (request.getEventLevel() != null && !request.getEventLevel().isBlank()
                && !Set.of("LOW", "MEDIUM", "HIGH", "CRITICAL").contains(request.getEventLevel().toUpperCase())) {
            throw new BusinessException("unsupported eventLevel: " + request.getEventLevel());
        }
        cacheManager.ensureTableLoaded(CachedTable.ANOMALY_TASK);
        cacheManager.ensureTableLoaded(CachedTable.INSTANCE_CONFIG);
        if (memoryCache.getAnomalyTask(request.getProjectId(), request.getTaskId()).isEmpty()
                || memoryCache.getInstanceBySequenceId(request.getProjectId(), request.getSequenceId()) == null) {
            throw new BusinessException("anomaly result query taskId or sequenceId does not belong to project");
        }
    }
}
