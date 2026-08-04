package com.sfkg.timeseries.service.impl;

import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.dto.EventQueryRequest;
import com.sfkg.timeseries.dto.EventSaveRequest;
import com.sfkg.timeseries.entity.TimeseriesEvent;
import com.sfkg.timeseries.mapper.TimeseriesEventMapper;
import com.sfkg.timeseries.service.TimeseriesEventService;
import com.sfkg.timeseries.vo.EventDetailVO;
import com.sfkg.timeseries.vo.EventListVO;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

@Service
public class TimeseriesEventServiceImpl implements TimeseriesEventService {

    private final TimeseriesEventMapper eventMapper;
    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesCacheManager cacheManager;

    public TimeseriesEventServiceImpl(
            TimeseriesEventMapper eventMapper,
            TimeseriesMemoryCache memoryCache,
            TimeseriesCacheManager cacheManager) {
        this.eventMapper = eventMapper;
        this.memoryCache = memoryCache;
        this.cacheManager = cacheManager;
    }

    @Override
    public List<EventListVO> listEvents(EventQueryRequest request) {
        cacheManager.ensureTableLoaded(CachedTable.EVENT);
        return memoryCache.listEvents().stream()
                .filter(entity -> matches(request, entity))
                .map(this::toListVO)
                .collect(Collectors.toList());
    }

    @Override
    public EventDetailVO getEventDetail(String eventId) {
        cacheManager.ensureTableLoaded(CachedTable.EVENT);
        return memoryCache.getEvent(eventId)
                .map(this::toDetailVO)
                .orElseGet(EventDetailVO::new);
    }

    @Override
    public String saveEvent(EventSaveRequest request) {
        cacheManager.ensureTableLoaded(CachedTable.EVENT);
        String eventId = request == null || request.getEventId() == null
                ? generateEventId()
                : request.getEventId();

        TimeseriesEvent entity = memoryCache.getEvent(eventId)
                .orElseGet(TimeseriesEvent::new);
        if (request != null) {
            BeanUtils.copyProperties(request, entity);
        }
        entity.setEventId(eventId);
        if (entity.getEventTime() == null) {
            entity.setEventTime(LocalDateTime.now());
        }

        eventMapper.insert(entity);
        memoryCache.putEvent(entity);
        return eventId;
    }

    @Override
    public String saveEventEntity(TimeseriesEvent entity) {
        if (entity == null) {
            return null;
        }
        cacheManager.ensureTableLoaded(CachedTable.EVENT);
        String eventId = entity.getEventId() != null
                ? entity.getEventId()
                : generateEventId();
        entity.setEventId(eventId);
        if (entity.getEventTime() == null) {
            entity.setEventTime(LocalDateTime.now());
        }
        LocalDateTime now = LocalDateTime.now();
        if (entity.getCreateTime() == null) {
            entity.setCreateTime(now);
        }
        entity.setUpdateTime(now);

        eventMapper.insert(entity);
        memoryCache.putEvent(entity);
        return eventId;
    }

    @Override
    public void validateEventRelations(EventSaveRequest request) {
        // TODO: Restore event relation validation here.
    }

    @Override
    public EventDetailVO enrichEventDetail(String eventId) {
        return getEventDetail(eventId);
    }

    @Override
    public void syncEventToGraph(String eventId) {
        // TODO: Restore graph synchronization here.
    }

    private String generateEventId() {
        return UUID.randomUUID().toString();
    }

    private EventListVO toListVO(TimeseriesEvent entity) {
        EventListVO vo = new EventListVO();
        BeanUtils.copyProperties(entity, vo);
        return vo;
    }

    private EventDetailVO toDetailVO(TimeseriesEvent entity) {
        EventDetailVO vo = new EventDetailVO();
        vo.setEventId(entity.getEventId());
        vo.setEventName(entity.getEventName());
        vo.setEventDescription(entity.getEventDescription());
        vo.setSemanticContext(buildSemanticContext(entity));
        return vo;
    }

    private Map<String, Object> buildSemanticContext(TimeseriesEvent entity) {
        Map<String, Object> context = new LinkedHashMap<>();
        context.put("eventType", entity.getEventType());
        context.put("eventSource", entity.getEventSource());
        context.put("eventLevel", entity.getEventLevel());
        context.put("eventTime", entity.getEventTime());
        context.put("relatedSequences", entity.getRelatedSequences());
        context.put("relatedRules", entity.getRelatedRules());
        context.put("confirmStatus", entity.getConfirmStatus());
        context.put("handleStatus", entity.getHandleStatus());
        context.put("diagnosisResult", entity.getDiagnosisResult());
        context.put("diagnosisBasis", entity.getDiagnosisBasis());
        context.put("disposalResult", entity.getDisposalResult());
        return context;
    }

    private boolean matches(EventQueryRequest request, TimeseriesEvent entity) {
        if (request == null) {
            return true;
        }
        return equalsTextIfPresent(request.getEventType(), entity.getEventType())
                && equalsTextIfPresent(request.getEventSource(), entity.getEventSource())
                && equalsTextIfPresent(request.getEventLevel(), entity.getEventLevel())
                && equalsTextIfPresent(request.getConfirmStatus(), entity.getConfirmStatus())
                && equalsTextIfPresent(request.getHandleStatus(), entity.getHandleStatus())
                && hasRelatedSequence(request, entity)
                && afterOrEqual(request.getStartTime(), entity.getEventTime())
                && beforeOrEqual(request.getEndTime(), entity.getEventTime());
    }

    private boolean hasRelatedSequence(EventQueryRequest request, TimeseriesEvent entity) {
        if (request.getRelatedSequences() == null || request.getRelatedSequences().isEmpty()) {
            return true;
        }
        return entity.getRelatedSequences() != null
                && request.getRelatedSequences().stream().anyMatch(entity.getRelatedSequences()::contains);
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
