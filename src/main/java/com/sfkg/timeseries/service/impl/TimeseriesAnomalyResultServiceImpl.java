package com.sfkg.timeseries.service.impl;

import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.client.AnomalyGrpcClient;
import com.sfkg.timeseries.dto.AnomalyResultQueryRequest;
import com.sfkg.timeseries.entity.TimeseriesEvent;
import com.sfkg.timeseries.mapper.TimeseriesEventMapper;
import com.sfkg.timeseries.service.TimeseriesAnomalyResultService;
import com.sfkg.timeseries.vo.AnomalyResultVO;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.stereotype.Service;

@Service
public class TimeseriesAnomalyResultServiceImpl implements TimeseriesAnomalyResultService {

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
        return memoryCache.listEvents().stream()
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
    public Integer createAnomalyEvent(AnomalyResultVO result) {
        Integer eventId = ThreadLocalRandom.current().nextInt(100000, 1000000);
        TimeseriesEvent event = new TimeseriesEvent();
        event.setEventId(eventId);
        event.setEventName("anomaly event " + eventId);
        event.setEventType("ANOMALY");
        event.setEventSource("ANOMALY_DETECTION");
        event.setEventLevel(result == null ? null : result.getAnomalyLevel());
        event.setRelatedSequences(result == null || result.getSequenceId() == null
                ? List.of()
                : List.of(result.getSequenceId()));
        event.setEventTime(LocalDateTime.now());

        eventMapper.insert(event);
        memoryCache.putEvent(event);
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

    private boolean sequenceMatches(Integer sequenceId, TimeseriesEvent event) {
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
}
