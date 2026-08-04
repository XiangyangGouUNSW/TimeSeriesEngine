package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.dto.EventQueryRequest;
import com.sfkg.timeseries.entity.TimeseriesEvent;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class TimeseriesEventFileMapper implements TimeseriesEventMapper {

    private final LocalJsonTableStore<TimeseriesEvent> store;

    public TimeseriesEventFileMapper(
            @Value("${timeseries.local-store-dir:data}") String storeDir) {
        this.store = new LocalJsonTableStore<>(
                storeDir,
                "timeseries-event.json",
                TimeseriesEvent.class);
    }

    @Override
    public void insert(TimeseriesEvent entity) {
        store.upsert(item -> sameBusinessKey(entity, item), entity);
    }

    @Override
    public void updateById(TimeseriesEvent entity) {
        insert(entity);
    }

    @Override
    public TimeseriesEvent selectById(String eventId) {
        return store.readAll().stream()
                .filter(entity -> Objects.equals(eventId, entity.getEventId()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public List<TimeseriesEvent> selectByCondition(Object condition) {
        return store.readAll().stream()
                .filter(entity -> matches(condition, entity))
                .collect(Collectors.toList());
    }

    @Override
    public void updateHandleStatus(String eventId, String status) {
        store.update(
                entity -> Objects.equals(eventId, entity.getEventId()),
                entity -> entity.setHandleStatus(status));
    }

    @Override
    public void updateDiagnosisResult(String eventId, String diagnosisResult) {
        store.update(
                entity -> Objects.equals(eventId, entity.getEventId()),
                entity -> entity.setDiagnosisResult(diagnosisResult));
    }

    @Override
    public void updateDisposalResult(String eventId, String disposalResult) {
        store.update(
                entity -> Objects.equals(eventId, entity.getEventId()),
                entity -> entity.setDisposalResult(disposalResult));
    }

    private boolean sameBusinessKey(TimeseriesEvent incoming, TimeseriesEvent stored) {
        return incoming != null
                && stored != null
                && Objects.equals(incoming.getEventId(), stored.getEventId());
    }

    private boolean matches(Object condition, TimeseriesEvent entity) {
        if (condition == null) {
            return true;
        }
        if (!(condition instanceof EventQueryRequest request)) {
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
