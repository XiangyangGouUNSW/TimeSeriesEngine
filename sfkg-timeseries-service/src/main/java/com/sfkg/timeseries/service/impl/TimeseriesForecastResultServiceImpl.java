package com.sfkg.timeseries.service.impl;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.stereotype.Service;

import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.client.ForecastGrpcClient;
import com.sfkg.timeseries.dto.ForecastResultQueryRequest;
import com.sfkg.timeseries.entity.TimeseriesEvent;
import com.sfkg.timeseries.mapper.TimeseriesEventMapper;
import com.sfkg.timeseries.service.TimeseriesForecastResultService;
import com.sfkg.timeseries.vo.ForecastResultVO;

@Service
public class TimeseriesForecastResultServiceImpl implements TimeseriesForecastResultService {

    private final TimeseriesEventMapper eventMapper;
    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesCacheManager cacheManager;
    private final ForecastGrpcClient forecastGrpcClient;

    public TimeseriesForecastResultServiceImpl(
            TimeseriesEventMapper eventMapper,
            TimeseriesMemoryCache memoryCache,
            TimeseriesCacheManager cacheManager,
            ForecastGrpcClient forecastGrpcClient) {
        this.eventMapper = eventMapper;
        this.memoryCache = memoryCache;
        this.cacheManager = cacheManager;
        this.forecastGrpcClient = forecastGrpcClient;
    }

    @Override
    public ForecastResultVO queryForecastResults(ForecastResultQueryRequest request) {
        return forecastGrpcClient.queryForecastResult(request);
    }

    @Override
    public ForecastResultVO handleForecastResult(Object rawResult) {
        return new ForecastResultVO();
    }

    @Override
    public String createWarningEvent(ForecastResultVO result) {
        String ts = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyMMddHHmmssSSS"));
        String taskId = result != null && result.getTaskId() != null ? result.getTaskId() : "UNKNOWN";
        String eventId = "EVT_FORECAST_" + taskId + "_" + ts;
        TimeseriesEvent event = new TimeseriesEvent();
        event.setProjectId(result != null ? result.getProjectId() : null);
        event.setEventId(eventId);
        event.setEventName("forecast event on " + taskId);
        event.setEventType("WARNING");
        event.setEventSource("FORECAST");
        event.setEventLevel(result == null ? null : result.getWarningLevel());
        event.setRelatedSequences(result == null || result.getSequenceId() == null
                ? List.of()
                : List.of(result.getSequenceId()));
        event.setEventTime(LocalDateTime.now());
        event.setTaskId(result != null ? result.getTaskId() : null);
        event.setConfirmStatus("PENDING");
        event.setHandleStatus("UNHANDLED");
        LocalDateTime now = LocalDateTime.now();
        event.setCreateTime(now);
        event.setUpdateTime(now);

        eventMapper.insert(event);
        memoryCache.computeEvent(event.getProjectId(), eventId, existing -> event);
        return eventId;
    }

    private boolean matches(ForecastResultQueryRequest request, TimeseriesEvent event) {
        if (event == null || !"FORECAST".equalsIgnoreCase(event.getEventSource())) {
            return false;
        }
        if (request == null) {
            return true;
        }
        return sequenceMatches(request.getSequenceId(), event)
                && afterOrEqual(request.getStartTime(), event.getEventTime())
                && beforeOrEqual(request.getEndTime(), event.getEventTime());
    }

    private ForecastResultVO toVO(ForecastResultQueryRequest request, TimeseriesEvent event) {
        ForecastResultVO vo = emptyVO(request);
        vo.setResultId(event.getEventId());
        vo.setWarningLevel(event.getEventLevel());
        if (vo.getSequenceId() == null && event.getRelatedSequences() != null && !event.getRelatedSequences().isEmpty()) {
            vo.setSequenceId(event.getRelatedSequences().iterator().next());
        }
        return vo;
    }

    private ForecastResultVO emptyVO(ForecastResultQueryRequest request) {
        ForecastResultVO vo = new ForecastResultVO();
        if (request != null) {
            vo.setTaskId(request.getTaskId());
            vo.setSequenceId(request.getSequenceId());
        }
        return vo;
    }

    private boolean sequenceMatches(String sequenceId, TimeseriesEvent event) {
        return sequenceId == null
                || (event.getRelatedSequences() != null && event.getRelatedSequences().contains(sequenceId));
    }

    private boolean afterOrEqual(LocalDateTime startTime, LocalDateTime actual) {
        return startTime == null || (actual != null && !actual.isBefore(startTime));
    }

    private boolean beforeOrEqual(LocalDateTime endTime, LocalDateTime actual) {
        return endTime == null || (actual != null && !actual.isAfter(endTime));
    }
}
