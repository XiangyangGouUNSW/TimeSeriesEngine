package com.sfkg.timeseries.service.impl;

import java.time.LocalDateTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.client.AnomalyGrpcClient;
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
    private final TimeseriesCacheManager cacheManager;
    private final AnomalyGrpcClient anomalyGrpcClient;

    public TimeseriesAnomalyResultServiceImpl(
            TimeseriesEventMapper eventMapper,
            TimeseriesMemoryCache memoryCache,
            TimeseriesCacheManager cacheManager,
            AnomalyGrpcClient anomalyGrpcClient) {
        this.eventMapper = eventMapper;
        this.memoryCache = memoryCache;
        this.cacheManager = cacheManager;
        this.anomalyGrpcClient = anomalyGrpcClient;
    }

    @Override
    public AnomalyResultVO queryAnomalyResults(AnomalyResultQueryRequest request) {
        cacheManager.ensureTableLoaded(CachedTable.EVENT);
        List<TimeseriesEvent> source = request != null && request.getProjectId() != null
                && !request.getProjectId().isBlank()
                ? memoryCache.listEvents(request.getProjectId())
                : memoryCache.listEvents();
        return source.stream()
                .filter(event -> matches(request, event))
                .findFirst()
                .map(event -> toVO(request, event))
                .orElseGet(() -> anomalyGrpcClient.queryAnomalyResult(request));
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
        String ts = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyMMddHHmmssSSS"));
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

    private boolean matches(AnomalyResultQueryRequest request, TimeseriesEvent event) {
        if (event == null || !"ANOMALY".equalsIgnoreCase(event.getEventType())) {
            return false;
        }
        if (request == null) {
            return true;
        }
        return sequenceMatches(request.getSequenceId(), event)
                && equalsTextIfPresent(request.getProjectId(), event.getProjectId())
                && equalsTextIfPresent(request.getEventLevel(), event.getEventLevel())
                && afterOrEqual(request.getStartTime(), event.getEventTime())
                && beforeOrEqual(request.getEndTime(), event.getEventTime());
    }

    private AnomalyResultVO toVO(AnomalyResultQueryRequest request, TimeseriesEvent event) {
        AnomalyResultVO vo = emptyVO(request);
        vo.setResultId(event.getEventId());
        vo.setAnomalyLevel(event.getEventLevel());
        if (vo.getSequenceId() == null && event.getRelatedSequences() != null && !event.getRelatedSequences().isEmpty()) {
            vo.setSequenceId(event.getRelatedSequences().iterator().next());
        }
        return vo;
    }

    private AnomalyResultVO emptyVO(AnomalyResultQueryRequest request) {
        AnomalyResultVO vo = new AnomalyResultVO();
        if (request != null) {
            vo.setTaskId(request.getTaskId());
            vo.setSequenceId(request.getSequenceId());
            vo.setAnomalyLevel(request.getEventLevel());
        }
        return vo;
    }

    private boolean sequenceMatches(String sequenceId, TimeseriesEvent event) {
        return sequenceId == null
                || (event.getRelatedSequences() != null && event.getRelatedSequences().contains(sequenceId));
    }

    private boolean equalsTextIfPresent(String expected, String actual) {
        return expected == null || (actual != null && expected.equalsIgnoreCase(actual));
    }

    private boolean afterOrEqual(LocalDateTime startTime, LocalDateTime actual) {
        return startTime == null || (actual != null && !actual.isBefore(startTime));
    }

    private boolean beforeOrEqual(LocalDateTime endTime, LocalDateTime actual) {
        return endTime == null || (actual != null && !actual.isAfter(endTime));
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
}
