package com.sfkg.timeseries.cache;

import com.sfkg.timeseries.entity.TimeseriesAnomalyResult;
import com.sfkg.timeseries.entity.TimeseriesAnomalyTask;
import com.sfkg.timeseries.entity.TimeseriesCategory;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.dto.HistoryDataQueryRequest;
import com.sfkg.timeseries.entity.TimeseriesDataPoint;
import com.sfkg.timeseries.entity.TimeseriesEvent;
import com.sfkg.timeseries.entity.TimeseriesForecastResult;
import com.sfkg.timeseries.entity.TimeseriesForecastTask;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.entity.TimeseriesRelation;
import com.sfkg.timeseries.entity.TimeseriesSyncLog;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

@Component
public class TimeseriesMemoryCache {

    private final Set<CachedTable> loadedTables = ConcurrentHashMap.newKeySet();
    private final List<TimeseriesInstanceConfig> instanceConfigs = new CopyOnWriteArrayList<>();
    private final List<TimeseriesCategory> categories = new CopyOnWriteArrayList<>();
    private final List<TimeseriesConstraint> constraints = new CopyOnWriteArrayList<>();
    private final List<TimeseriesRelation> relations = new CopyOnWriteArrayList<>();
    private final List<TimeseriesEvent> events = new CopyOnWriteArrayList<>();
    private final List<TimeseriesAnomalyTask> anomalyTasks = new CopyOnWriteArrayList<>();
    private final List<TimeseriesForecastTask> forecastTasks = new CopyOnWriteArrayList<>();
    private final List<TimeseriesAnomalyResult> anomalyResults = new CopyOnWriteArrayList<>();
    private final List<TimeseriesForecastResult> forecastResults = new CopyOnWriteArrayList<>();
    private final List<TimeseriesSyncLog> syncLogs = new CopyOnWriteArrayList<>();
    private final List<TimeseriesDataPoint> dataPoints = new CopyOnWriteArrayList<>();

    // ── Map indexes for fast lookup ───────────────────────────────────
    private final Map<String, TimeseriesInstanceConfig> instanceBySequenceId = new ConcurrentHashMap<>();
    private final Map<String, TimeseriesConstraint> constraintByConstraintId = new ConcurrentHashMap<>();
    private final Map<String, List<TimeseriesConstraint>> constraintsByCategoryId = new ConcurrentHashMap<>();
    private final Map<String, TimeseriesRelation> relationByRelationId = new ConcurrentHashMap<>();
    private final Map<String, List<TimeseriesRelation>> relationsByTargetSequenceId = new ConcurrentHashMap<>();
    private final Map<String, List<TimeseriesRelation>> relationsBySourceSequenceId = new ConcurrentHashMap<>();
    private final Map<String, TimeseriesEvent> eventsByEventId = new ConcurrentHashMap<>();
    private final Map<String, List<TimeseriesAnomalyResult>> anomalyResultsByTaskId = new ConcurrentHashMap<>();
    private final Map<String, List<TimeseriesForecastResult>> forecastResultsByTaskId = new ConcurrentHashMap<>();

    public boolean isLoaded(CachedTable table) {
        return loadedTables.contains(table);
    }

    public void markLoaded(CachedTable table) {
        loadedTables.add(table);
    }

    public void markUnloaded(CachedTable table) {
        loadedTables.remove(table);
    }

    public void evict(CachedTable table) {
        switch (table) {
            case INSTANCE_CONFIG -> { instanceConfigs.clear(); instanceBySequenceId.clear(); }
            case CATEGORY -> categories.clear();
            case CONSTRAINT -> { constraints.clear(); constraintByConstraintId.clear(); constraintsByCategoryId.clear(); }
            case RELATION -> { relations.clear(); relationByRelationId.clear(); relationsByTargetSequenceId.clear(); relationsBySourceSequenceId.clear(); }
            case EVENT -> { events.clear(); eventsByEventId.clear(); }
            case ANOMALY_TASK -> anomalyTasks.clear();
            case FORECAST_TASK -> forecastTasks.clear();
            case ANOMALY_RESULT -> { anomalyResults.clear(); anomalyResultsByTaskId.clear(); }
            case FORECAST_RESULT -> { forecastResults.clear(); forecastResultsByTaskId.clear(); }
            case SYNC_LOG -> syncLogs.clear();
            case TIMESERIES_DATA -> dataPoints.clear();
        }
        markUnloaded(table);
    }

    public void replaceInstanceConfigs(Collection<TimeseriesInstanceConfig> entities) {
        replaceAll(instanceConfigs, entities);
        rebuildInstanceIndex();
        markLoaded(CachedTable.INSTANCE_CONFIG);
    }

    public void putInstanceConfig(TimeseriesInstanceConfig entity) {
        if (entity != null && entity.getSequenceId() != null) {
            upsert(instanceConfigs, item -> entity.getSequenceId().equals(item.getSequenceId()), entity);
            instanceBySequenceId.put(entity.getSequenceId(), entity);
            markLoaded(CachedTable.INSTANCE_CONFIG);
        }
    }

    public Optional<TimeseriesInstanceConfig> getInstanceConfig(String sequenceId) {
        return instanceConfigs.stream()
                .filter(entity -> equalsValue(sequenceId, entity.getSequenceId()))
                .findFirst();
    }

    public List<TimeseriesInstanceConfig> listInstanceConfigs() {
        return List.copyOf(instanceConfigs);
    }

    public void replaceCategories(Collection<TimeseriesCategory> entities) {
        replaceAll(categories, entities);
        markLoaded(CachedTable.CATEGORY);
    }

    public void putCategory(TimeseriesCategory entity) {
        if (entity != null && entity.getCategoryId() != null) {
            upsert(categories, item -> entity.getCategoryId().equals(item.getCategoryId()), entity);
            markLoaded(CachedTable.CATEGORY);
        }
    }

    public Optional<TimeseriesCategory> getCategory(String categoryId) {
        return categories.stream()
                .filter(entity -> equalsValue(categoryId, entity.getCategoryId()))
                .findFirst();
    }

    public List<TimeseriesCategory> listCategories() {
        return List.copyOf(categories);
    }

    public void replaceConstraints(Collection<TimeseriesConstraint> entities) {
        replaceAll(constraints, entities);
        rebuildConstraintIndexes();
        markLoaded(CachedTable.CONSTRAINT);
    }

    public void putConstraint(TimeseriesConstraint entity) {
        if (entity != null && entity.getConstraintId() != null) {
            upsert(constraints, item -> entity.getConstraintId().equals(item.getConstraintId()), entity);
            constraintByConstraintId.put(entity.getConstraintId(), entity);
            rebuildConstraintsByCategoryId();
            markLoaded(CachedTable.CONSTRAINT);
        }
    }

    public Optional<TimeseriesConstraint> getConstraint(String constraintId) {
        return constraints.stream()
                .filter(entity -> equalsValue(constraintId, entity.getConstraintId()))
                .findFirst();
    }

    public List<TimeseriesConstraint> listConstraints() {
        return List.copyOf(constraints);
    }

    public void replaceRelations(Collection<TimeseriesRelation> entities) {
        replaceAll(relations, entities);
        rebuildRelationIndexes();
        markLoaded(CachedTable.RELATION);
    }

    public void putRelation(TimeseriesRelation entity) {
        if (entity != null && entity.getRelationId() != null) {
            upsert(relations, item -> entity.getRelationId().equals(item.getRelationId()), entity);
            relationByRelationId.put(entity.getRelationId(), entity);
            rebuildRelationsByTargetId();
            rebuildRelationsBySourceId();
            markLoaded(CachedTable.RELATION);
        }
    }

    public Optional<TimeseriesRelation> getRelation(String relationId) {
        return relations.stream()
                .filter(entity -> equalsValue(relationId, entity.getRelationId()))
                .findFirst();
    }

    public List<TimeseriesRelation> listRelations() {
        return List.copyOf(relations);
    }

    public void replaceEvents(Collection<TimeseriesEvent> entities) {
        replaceAll(events, entities);
        eventsByEventId.clear();
        if (entities != null) {
            entities.forEach(e -> { if (e.getEventId() != null) eventsByEventId.put(e.getEventId(), e); });
        }
        markLoaded(CachedTable.EVENT);
    }

    public void putEvent(TimeseriesEvent entity) {
        if (entity != null && entity.getEventId() != null) {
            upsert(events, item -> entity.getEventId().equals(item.getEventId()), entity);
            eventsByEventId.put(entity.getEventId(), entity);
            markLoaded(CachedTable.EVENT);
        }
    }

    public Optional<TimeseriesEvent> getEvent(String eventId) {
        return events.stream()
                .filter(entity -> equalsValue(eventId, entity.getEventId()))
                .findFirst();
    }

    public List<TimeseriesEvent> listEvents() {
        return List.copyOf(events);
    }

    public void replaceAnomalyTasks(Collection<TimeseriesAnomalyTask> entities) {
        replaceAll(anomalyTasks, entities);
        markLoaded(CachedTable.ANOMALY_TASK);
    }

    public void putAnomalyTask(TimeseriesAnomalyTask entity) {
        if (entity != null && entity.getTaskId() != null) {
            upsert(anomalyTasks, item -> entity.getTaskId().equals(item.getTaskId()), entity);
            markLoaded(CachedTable.ANOMALY_TASK);
        }
    }

    public Optional<TimeseriesAnomalyTask> getAnomalyTask(String taskId) {
        return anomalyTasks.stream()
                .filter(entity -> equalsValue(taskId, entity.getTaskId()))
                .findFirst();
    }

    public List<TimeseriesAnomalyTask> listAnomalyTasks() {
        return List.copyOf(anomalyTasks);
    }

    public void replaceForecastTasks(Collection<TimeseriesForecastTask> entities) {
        replaceAll(forecastTasks, entities);
        markLoaded(CachedTable.FORECAST_TASK);
    }

    public void putForecastTask(TimeseriesForecastTask entity) {
        if (entity != null && entity.getTaskId() != null) {
            upsert(forecastTasks, item -> entity.getTaskId().equals(item.getTaskId()), entity);
            markLoaded(CachedTable.FORECAST_TASK);
        }
    }

    public Optional<TimeseriesForecastTask> getForecastTask(String taskId) {
        return forecastTasks.stream()
                .filter(entity -> equalsValue(taskId, entity.getTaskId()))
                .findFirst();
    }

    public List<TimeseriesForecastTask> listForecastTasks() {
        return List.copyOf(forecastTasks);
    }

    public void replaceAnomalyResults(Collection<TimeseriesAnomalyResult> entities) {
        replaceAll(anomalyResults, entities);
        rebuildAnomalyResultsByTaskId();
        markLoaded(CachedTable.ANOMALY_RESULT);
    }

    public void putAnomalyResult(TimeseriesAnomalyResult entity) {
        if (entity != null && entity.getResultId() != null) {
            upsert(anomalyResults, item -> entity.getResultId().equals(item.getResultId()), entity);
            rebuildAnomalyResultsByTaskId();
            markLoaded(CachedTable.ANOMALY_RESULT);
        }
    }

    public List<TimeseriesAnomalyResult> listAnomalyResults() {
        return List.copyOf(anomalyResults);
    }

    public void replaceForecastResults(Collection<TimeseriesForecastResult> entities) {
        replaceAll(forecastResults, entities);
        rebuildForecastResultsByTaskId();
        markLoaded(CachedTable.FORECAST_RESULT);
    }

    public void putForecastResult(TimeseriesForecastResult entity) {
        if (entity != null && entity.getResultId() != null) {
            upsert(forecastResults, item -> entity.getResultId().equals(item.getResultId()), entity);
            rebuildForecastResultsByTaskId();
            markLoaded(CachedTable.FORECAST_RESULT);
        }
    }

    public List<TimeseriesForecastResult> listForecastResults() {
        return List.copyOf(forecastResults);
    }

    public void replaceSyncLogs(Collection<TimeseriesSyncLog> entities) {
        replaceAll(syncLogs, entities);
        markLoaded(CachedTable.SYNC_LOG);
    }

    public void putSyncLog(TimeseriesSyncLog entity) {
        if (entity != null && entity.getId() != null) {
            upsert(syncLogs, item -> entity.getId().equals(item.getId()), entity);
            markLoaded(CachedTable.SYNC_LOG);
        }
    }

    public List<TimeseriesSyncLog> listSyncLogs() {
        return List.copyOf(syncLogs);
    }

    public void replaceTimeseriesDataPoints(Collection<TimeseriesDataPoint> points) {
        replaceAll(dataPoints, points);
        markLoaded(CachedTable.TIMESERIES_DATA);
    }

    public void replaceTimeseriesDataPoints(String sequenceId, Collection<TimeseriesDataPoint> points) {
        if (sequenceId == null) {
            replaceTimeseriesDataPoints(points);
            return;
        }
        dataPoints.removeIf(point -> equalsValue(sequenceId, point.getSequenceId()));
        if (points != null) {
            dataPoints.addAll(points);
        }
        markLoaded(CachedTable.TIMESERIES_DATA);
    }

    public void putTimeseriesDataPoints(Collection<TimeseriesDataPoint> points) {
        if (points == null || points.isEmpty()) {
            return;
        }
        for (TimeseriesDataPoint point : points) {
            if (point != null && point.getSequenceId() != null && point.getTimestamp() != null) {
                upsert(
                        dataPoints,
                        item -> point.getSequenceId().equals(item.getSequenceId())
                                && point.getTimestamp().equals(item.getTimestamp()),
                        point);
            }
        }
        markLoaded(CachedTable.TIMESERIES_DATA);
    }

    public List<TimeseriesDataPoint> listTimeseriesDataPoints(HistoryDataQueryRequest request) {
        return dataPoints.stream()
                .filter(point -> matchesDataQuery(request, point))
                .sorted(Comparator
                        .comparing(TimeseriesDataPoint::getSequenceId, Comparator.nullsLast(String::compareTo))
                        .thenComparing(TimeseriesDataPoint::getTimestamp, Comparator.nullsLast(java.time.LocalDateTime::compareTo)))
                .collect(Collectors.toList());
    }

    // ── Indexed query methods ────────────────────────────────────────

    public TimeseriesInstanceConfig getInstanceBySequenceId(String sequenceId) {
        return sequenceId != null ? instanceBySequenceId.get(sequenceId) : null;
    }

    public List<TimeseriesConstraint> listConstraintsByCategoryId(String categoryId) {
        return categoryId != null && constraintsByCategoryId.containsKey(categoryId)
                ? List.copyOf(constraintsByCategoryId.get(categoryId))
                : List.of();
    }

    public TimeseriesRelation getRelationByRelationId(String relationId) {
        return relationId != null ? relationByRelationId.get(relationId) : null;
    }

    public List<TimeseriesRelation> listRelationsByTargetSequenceId(String sequenceId) {
        return sequenceId != null && relationsByTargetSequenceId.containsKey(sequenceId)
                ? List.copyOf(relationsByTargetSequenceId.get(sequenceId))
                : List.of();
    }

    public List<TimeseriesRelation> listRelationsBySourceSequenceId(String sequenceId) {
        return sequenceId != null && relationsBySourceSequenceId.containsKey(sequenceId)
                ? List.copyOf(relationsBySourceSequenceId.get(sequenceId))
                : List.of();
    }

    public TimeseriesEvent getEventByEventId(String eventId) {
        return eventId != null ? eventsByEventId.get(eventId) : null;
    }

    public List<TimeseriesAnomalyResult> listAnomalyResultsByTaskId(String taskId) {
        return taskId != null && anomalyResultsByTaskId.containsKey(taskId)
                ? List.copyOf(anomalyResultsByTaskId.get(taskId))
                : List.of();
    }

    public List<TimeseriesForecastResult> listForecastResultsByTaskId(String taskId) {
        return taskId != null && forecastResultsByTaskId.containsKey(taskId)
                ? List.copyOf(forecastResultsByTaskId.get(taskId))
                : List.of();
    }

    // ── Internal index rebuild helpers ────────────────────────────────

    private void rebuildInstanceIndex() {
        instanceBySequenceId.clear();
        instanceConfigs.forEach(c -> {
            if (c.getSequenceId() != null) instanceBySequenceId.put(c.getSequenceId(), c);
        });
    }

    private void rebuildConstraintIndexes() {
        constraintByConstraintId.clear();
        constraintsByCategoryId.clear();
        constraints.forEach(c -> {
            if (c.getConstraintId() != null) constraintByConstraintId.put(c.getConstraintId(), c);
        });
        rebuildConstraintsByCategoryId();
    }

    private void rebuildConstraintsByCategoryId() {
        constraintsByCategoryId.clear();
        constraints.forEach(c -> {
            if (c.getCategoryId() != null) {
                constraintsByCategoryId.computeIfAbsent(c.getCategoryId(), k -> new CopyOnWriteArrayList<>()).add(c);
            }
        });
    }

    private void rebuildRelationIndexes() {
        relationByRelationId.clear();
        relations.forEach(r -> {
            if (r.getRelationId() != null) relationByRelationId.put(r.getRelationId(), r);
        });
        rebuildRelationsByTargetId();
        rebuildRelationsBySourceId();
    }

    private void rebuildRelationsByTargetId() {
        relationsByTargetSequenceId.clear();
        relations.forEach(r -> {
            if (r.getTargetSequenceId() != null) {
                relationsByTargetSequenceId.computeIfAbsent(r.getTargetSequenceId(), k -> new CopyOnWriteArrayList<>()).add(r);
            }
        });
    }

    private void rebuildRelationsBySourceId() {
        relationsBySourceSequenceId.clear();
        relations.forEach(r -> {
            if (r.getSourceSequences() != null) {
                r.getSourceSequences().forEach(src -> {
                    if (src != null) {
                        relationsBySourceSequenceId.computeIfAbsent(src, k -> new CopyOnWriteArrayList<>()).add(r);
                    }
                });
            }
        });
    }

    private void rebuildAnomalyResultsByTaskId() {
        anomalyResultsByTaskId.clear();
        anomalyResults.forEach(r -> {
            // AnomalyResult may not have taskId; use source + sequenceIds as key
            if (r.getSource() != null) {
                anomalyResultsByTaskId.computeIfAbsent(r.getSource(), k -> new CopyOnWriteArrayList<>()).add(r);
            }
            if (r.getSequenceIds() != null) {
                r.getSequenceIds().forEach(sid -> {
                    if (sid != null) {
                        anomalyResultsByTaskId.computeIfAbsent(sid, k -> new CopyOnWriteArrayList<>()).add(r);
                    }
                });
            }
        });
    }

    private void rebuildForecastResultsByTaskId() {
        forecastResultsByTaskId.clear();
        forecastResults.forEach(r -> {
            if (r.getTaskId() != null) {
                forecastResultsByTaskId.computeIfAbsent(r.getTaskId(), k -> new CopyOnWriteArrayList<>()).add(r);
            }
        });
    }

    // ── Private helpers ───────────────────────────────────────────────

    private <T> void replaceAll(List<T> target, Collection<T> records) {
        target.clear();
        if (records != null) {
            target.addAll(records);
        }
    }

    private <T> void upsert(List<T> records, Predicate<T> sameBusinessKey, T entity) {
        records.removeIf(sameBusinessKey);
        records.add(entity);
    }

    private boolean equalsValue(String left, String right) {
        return left != null && left.equals(right);
    }

    private boolean matchesDataQuery(HistoryDataQueryRequest request, TimeseriesDataPoint point) {
        if (point == null) {
            return false;
        }
        if (request == null) {
            return true;
        }
        if (request.getSequenceId() != null && !request.getSequenceId().equals(point.getSequenceId())) {
            return false;
        }
        if (request.getStartTime() != null
                && (point.getTimestamp() == null || point.getTimestamp().isBefore(request.getStartTime()))) {
            return false;
        }
        return request.getEndTime() == null
                || (point.getTimestamp() != null && !point.getTimestamp().isAfter(request.getEndTime()));
    }
}
