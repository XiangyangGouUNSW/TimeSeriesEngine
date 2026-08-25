package com.sfkg.timeseries.service.impl;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.cache.TimeseriesMemoryCache;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.common.ProjectIdValidator;
import com.sfkg.timeseries.common.SemanticId;
import com.sfkg.timeseries.dto.EventQueryRequest;
import com.sfkg.timeseries.dto.EventSaveRequest;
import com.sfkg.timeseries.entity.TimeseriesEvent;
import com.sfkg.timeseries.mapper.TimeseriesEventMapper;
import com.sfkg.timeseries.service.TimeseriesEventService;
import com.sfkg.timeseries.vo.EventDetailVO;
import com.sfkg.timeseries.vo.EventListVO;

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
        List<TimeseriesEvent> source = request != null && request.getProjectId() != null
                && !request.getProjectId().isBlank()
                ? memoryCache.listEvents(request.getProjectId())
                : memoryCache.listEvents();
        return source.stream()
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
    public EventDetailVO getEventDetail(String projectId, String eventId) {
        cacheManager.ensureTableLoaded(CachedTable.EVENT);
        return memoryCache.getEvent(projectId, eventId)
                .map(this::toDetailVO)
                .orElseGet(EventDetailVO::new);
    }

    @Override
    public String saveEvent(EventSaveRequest request) {
        if (request == null) {
            throw new BusinessException("event request must not be null");
        }
        request.setProjectId(ProjectIdValidator.require(request.getProjectId()));
        if (request.getEventName() == null || request.getEventName().isBlank()) {
            throw new BusinessException("eventName must not be empty");
        }
        if (request.getEventType() == null || request.getEventType().isBlank()) {
            throw new BusinessException("eventType must not be empty");
        }
        if (request.getEventLevel() == null || request.getEventLevel().isBlank()) {
            throw new BusinessException("eventLevel must not be empty");
        }
        cacheManager.ensureTableLoaded(CachedTable.EVENT);
        String eventId = request.getEventId() == null
                ? generateEventId(request.getEventType(), request.getEventName())
                : request.getEventId();

        TimeseriesEvent entity = memoryCache.computeEvent(
                request != null ? request.getProjectId() : null, eventId, existing -> {
            TimeseriesEvent e = existing != null ? existing : new TimeseriesEvent();
            if (request != null) {
                BeanUtils.copyProperties(request, e);
            }
            e.setEventId(eventId);
            e.setProjectId(request != null ? request.getProjectId() : (existing != null ? existing.getProjectId() : null));
            if (e.getEventTime() == null) {
                e.setEventTime(LocalDateTime.now());
            }
            // audit fields
            LocalDateTime now = LocalDateTime.now();
            String user = request != null ? request.getUser() : null;
            if (existing == null) {
                e.setCreateTime(now);
                e.setCreateUser(user);
            } else {
                e.setCreateTime(e.getCreateTime() != null ? e.getCreateTime() : now);
                e.setCreateUser(e.getCreateUser());
            }
            e.setUpdateTime(now);
            e.setUpdateUser(user);
            return e;
        });

        eventMapper.insert(entity);
        return eventId;
    }

    @Override
    public String saveEventEntity(TimeseriesEvent entity) {
        if (entity == null) {
            return null;
        }
        entity.setProjectId(ProjectIdValidator.require(entity.getProjectId()));
        cacheManager.ensureTableLoaded(CachedTable.EVENT);
        String eventId = entity.getEventId() != null
                ? entity.getEventId()
                : generateEventId(entity.getEventType(), entity.getEventName());

        TimeseriesEvent merged = memoryCache.computeEvent(entity.getProjectId(), eventId, existing -> {
            TimeseriesEvent e = existing != null ? existing : new TimeseriesEvent();
            BeanUtils.copyProperties(entity, e);
            e.setEventId(eventId);
            if (e.getEventTime() == null) {
                e.setEventTime(LocalDateTime.now());
            }
            LocalDateTime now = LocalDateTime.now();
            if (e.getCreateTime() == null) {
                e.setCreateTime(now);
            }
            e.setUpdateTime(now);
            // preserve createUser if existing, don't overwrite with null
            if (e.getCreateUser() == null && existing != null) {
                e.setCreateUser(existing.getCreateUser());
            }
            return e;
        });

        eventMapper.insert(merged);
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

    private String generateEventId(String eventType, String eventName) {
        return SemanticId.generate(eventType, eventName);
    }

    private EventListVO toListVO(TimeseriesEvent entity) {
        EventListVO vo = new EventListVO();
        BeanUtils.copyProperties(entity, vo);
        return vo;
    }

    private EventDetailVO toDetailVO(TimeseriesEvent entity) {
        EventDetailVO vo = new EventDetailVO();
        vo.setEventId(entity.getEventId());
        vo.setProjectId(entity.getProjectId());
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
                && equalsTextIfPresent(request.getProjectId(), entity.getProjectId())
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
