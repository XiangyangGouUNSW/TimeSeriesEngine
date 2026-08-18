package com.sfkg.timeseries.cache;

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

import com.sfkg.timeseries.dto.HistoryDataQueryRequest;
import com.sfkg.timeseries.entity.TimeseriesAnomalyResult;
import com.sfkg.timeseries.entity.TimeseriesAnomalyTask;
import com.sfkg.timeseries.entity.TimeseriesCategory;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.entity.TimeseriesDataPoint;
import com.sfkg.timeseries.entity.TimeseriesEvent;
import com.sfkg.timeseries.entity.TimeseriesForecastResult;
import com.sfkg.timeseries.entity.TimeseriesForecastTask;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.entity.TimeseriesRelation;
import com.sfkg.timeseries.entity.TimeseriesSyncLog;

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
    private final Map<String, TimeseriesRelation> relationByRelationId = new ConcurrentHashMap<>();
    private final Map<String, List<TimeseriesRelation>> relationsByTargetSequenceId = new ConcurrentHashMap<>();
    private final Map<String, List<TimeseriesRelation>> relationsBySourceSequenceId = new ConcurrentHashMap<>();
    private final Map<String, TimeseriesEvent> eventsByEventId = new ConcurrentHashMap<>();
    private final Map<String, List<TimeseriesAnomalyResult>> anomalyResultsByTaskId = new ConcurrentHashMap<>();
    private final Map<String, List<TimeseriesForecastResult>> forecastResultsByTaskId = new ConcurrentHashMap<>();

    // ── Concurrency locks (one per entity type to minimise contention) ─
    private final Object instanceLock = new Object();
    private final Object categoryLock = new Object();
    private final Object constraintLock = new Object();
    private final Object relationLock = new Object();
    private final Object eventLock = new Object();
    private final Object anomalyTaskLock = new Object();
    private final Object forecastTaskLock = new Object();
    private final Object anomalyResultLock = new Object();
    private final Object forecastResultLock = new Object();
    private final Object syncLogLock = new Object();
    private final Object dataPointLock = new Object();

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
            case CONSTRAINT -> { constraints.clear(); constraintByConstraintId.clear(); }
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
        synchronized (instanceLock) {
            replaceAll(instanceConfigs, entities);
            rebuildInstanceIndex();
            markLoaded(CachedTable.INSTANCE_CONFIG);
        }
    }

    /**
     * Atomically get-or-create + modify + store an InstanceConfig.
     * The {@code updater} receives the existing entity (or {@code null} if absent)
     * and must return the entity to store (must not be null).
     */
    public TimeseriesInstanceConfig computeInstanceConfig(String sequenceId,
            java.util.function.Function<TimeseriesInstanceConfig, TimeseriesInstanceConfig> updater) {
        synchronized (instanceLock) {
            TimeseriesInstanceConfig existing = getInstanceConfig(sequenceId).orElse(null);
            TimeseriesInstanceConfig result = updater.apply(existing);
            if (result != null && result.getSequenceId() != null) {
                upsert(instanceConfigs, item -> result.getSequenceId().equals(item.getSequenceId()), result);
                instanceBySequenceId.put(result.getSequenceId(), result);
                markLoaded(CachedTable.INSTANCE_CONFIG);
            }
            return result;
        }
    }

    public void putInstanceConfig(TimeseriesInstanceConfig entity) {
        if (entity != null && entity.getSequenceId() != null) {
            synchronized (instanceLock) {
                upsert(instanceConfigs, item -> entity.getSequenceId().equals(item.getSequenceId()), entity);
                instanceBySequenceId.put(entity.getSequenceId(), entity);
                markLoaded(CachedTable.INSTANCE_CONFIG);
            }
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
        synchronized (categoryLock) {
            replaceAll(categories, entities);
            markLoaded(CachedTable.CATEGORY);
        }
    }

    public TimeseriesCategory computeCategory(String categoryId,
            java.util.function.Function<TimeseriesCategory, TimeseriesCategory> updater) {
        synchronized (categoryLock) {
            TimeseriesCategory existing = getCategory(categoryId).orElse(null);
            TimeseriesCategory result = updater.apply(existing);
            if (result != null && result.getCategoryId() != null) {
                upsert(categories, item -> result.getCategoryId().equals(item.getCategoryId()), result);
                markLoaded(CachedTable.CATEGORY);
            }
            return result;
        }
    }

    public void putCategory(TimeseriesCategory entity) {
        if (entity != null && entity.getCategoryId() != null) {
            synchronized (categoryLock) {
                upsert(categories, item -> entity.getCategoryId().equals(item.getCategoryId()), entity);
                markLoaded(CachedTable.CATEGORY);
            }
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
        synchronized (constraintLock) {
            replaceAll(constraints, entities);
            rebuildConstraintIndexes();
            markLoaded(CachedTable.CONSTRAINT);
        }
    }

    public TimeseriesConstraint computeConstraint(String constraintId,
            java.util.function.Function<TimeseriesConstraint, TimeseriesConstraint> updater) {
        synchronized (constraintLock) {
            TimeseriesConstraint existing = getConstraint(constraintId).orElse(null);
            TimeseriesConstraint result = updater.apply(existing);
            if (result != null && result.getConstraintId() != null) {
                upsert(constraints, item -> result.getConstraintId().equals(item.getConstraintId()), result);
                constraintByConstraintId.put(result.getConstraintId(), result);
                markLoaded(CachedTable.CONSTRAINT);
            }
            return result;
        }
    }

    public void putConstraint(TimeseriesConstraint entity) {
        if (entity != null && entity.getConstraintId() != null) {
            synchronized (constraintLock) {
                upsert(constraints, item -> entity.getConstraintId().equals(item.getConstraintId()), entity);
                constraintByConstraintId.put(entity.getConstraintId(), entity);
                markLoaded(CachedTable.CONSTRAINT);
            }
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
        synchronized (relationLock) {
            replaceAll(relations, entities);
            rebuildRelationIndexes();
            markLoaded(CachedTable.RELATION);
        }
    }

    public TimeseriesRelation computeRelation(String relationId,
            java.util.function.Function<TimeseriesRelation, TimeseriesRelation> updater) {
        synchronized (relationLock) {
            TimeseriesRelation existing = getRelation(relationId).orElse(null);
            TimeseriesRelation result = updater.apply(existing);
            if (result != null && result.getRelationId() != null) {
                upsert(relations, item -> result.getRelationId().equals(item.getRelationId()), result);
                relationByRelationId.put(result.getRelationId(), result);
                rebuildRelationsByTargetId();
                rebuildRelationsBySourceId();
                markLoaded(CachedTable.RELATION);
            }
            return result;
        }
    }

    public void putRelation(TimeseriesRelation entity) {
        if (entity != null && entity.getRelationId() != null) {
            synchronized (relationLock) {
                upsert(relations, item -> entity.getRelationId().equals(item.getRelationId()), entity);
                relationByRelationId.put(entity.getRelationId(), entity);
                rebuildRelationsByTargetId();
                rebuildRelationsBySourceId();
                markLoaded(CachedTable.RELATION);
            }
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
        synchronized (eventLock) {
            replaceAll(events, entities);
            eventsByEventId.clear();
            if (entities != null) {
                entities.forEach(e -> { if (e.getEventId() != null) eventsByEventId.put(e.getEventId(), e); });
            }
            markLoaded(CachedTable.EVENT);
        }
    }

    public TimeseriesEvent computeEvent(String eventId,
            java.util.function.Function<TimeseriesEvent, TimeseriesEvent> updater) {
        synchronized (eventLock) {
            TimeseriesEvent existing = getEvent(eventId).orElse(null);
            TimeseriesEvent result = updater.apply(existing);
            if (result != null && result.getEventId() != null) {
                upsert(events, item -> result.getEventId().equals(item.getEventId()), result);
                eventsByEventId.put(result.getEventId(), result);
                markLoaded(CachedTable.EVENT);
            }
            return result;
        }
    }

    public void putEvent(TimeseriesEvent entity) {
        if (entity != null && entity.getEventId() != null) {
            synchronized (eventLock) {
                upsert(events, item -> entity.getEventId().equals(item.getEventId()), entity);
                eventsByEventId.put(entity.getEventId(), entity);
                markLoaded(CachedTable.EVENT);
            }
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
        synchronized (anomalyTaskLock) {
            replaceAll(anomalyTasks, entities);
            markLoaded(CachedTable.ANOMALY_TASK);
        }
    }

    public TimeseriesAnomalyTask computeAnomalyTask(String taskId,
            java.util.function.Function<TimeseriesAnomalyTask, TimeseriesAnomalyTask> updater) {
        synchronized (anomalyTaskLock) {
            TimeseriesAnomalyTask existing = getAnomalyTask(taskId).orElse(null);
            TimeseriesAnomalyTask result = updater.apply(existing);
            if (result != null && result.getTaskId() != null) {
                upsert(anomalyTasks, item -> result.getTaskId().equals(item.getTaskId()), result);
                markLoaded(CachedTable.ANOMALY_TASK);
            }
            return result;
        }
    }

    public void putAnomalyTask(TimeseriesAnomalyTask entity) {
        if (entity != null && entity.getTaskId() != null) {
            synchronized (anomalyTaskLock) {
                upsert(anomalyTasks, item -> entity.getTaskId().equals(item.getTaskId()), entity);
                markLoaded(CachedTable.ANOMALY_TASK);
            }
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
        synchronized (forecastTaskLock) {
            replaceAll(forecastTasks, entities);
            markLoaded(CachedTable.FORECAST_TASK);
        }
    }

    public TimeseriesForecastTask computeForecastTask(String taskId,
            java.util.function.Function<TimeseriesForecastTask, TimeseriesForecastTask> updater) {
        synchronized (forecastTaskLock) {
            TimeseriesForecastTask existing = getForecastTask(taskId).orElse(null);
            TimeseriesForecastTask result = updater.apply(existing);
            if (result != null && result.getTaskId() != null) {
                upsert(forecastTasks, item -> result.getTaskId().equals(item.getTaskId()), result);
                markLoaded(CachedTable.FORECAST_TASK);
            }
            return result;
        }
    }

    public void putForecastTask(TimeseriesForecastTask entity) {
        if (entity != null && entity.getTaskId() != null) {
            synchronized (forecastTaskLock) {
                upsert(forecastTasks, item -> entity.getTaskId().equals(item.getTaskId()), entity);
                markLoaded(CachedTable.FORECAST_TASK);
            }
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
        synchronized (anomalyResultLock) {
            replaceAll(anomalyResults, entities);
            rebuildAnomalyResultsByTaskId();
            markLoaded(CachedTable.ANOMALY_RESULT);
        }
    }

    public void putAnomalyResult(TimeseriesAnomalyResult entity) {
        if (entity != null && entity.getResultId() != null) {
            synchronized (anomalyResultLock) {
                upsert(anomalyResults, item -> entity.getResultId().equals(item.getResultId()), entity);
                rebuildAnomalyResultsByTaskId();
                markLoaded(CachedTable.ANOMALY_RESULT);
            }
        }
    }

    public List<TimeseriesAnomalyResult> listAnomalyResults() {
        return List.copyOf(anomalyResults);
    }

    public void replaceForecastResults(Collection<TimeseriesForecastResult> entities) {
        synchronized (forecastResultLock) {
            replaceAll(forecastResults, entities);
            rebuildForecastResultsByTaskId();
            markLoaded(CachedTable.FORECAST_RESULT);
        }
    }

    public void putForecastResult(TimeseriesForecastResult entity) {
        if (entity != null && entity.getResultId() != null) {
            synchronized (forecastResultLock) {
                upsert(forecastResults, item -> entity.getResultId().equals(item.getResultId()), entity);
                rebuildForecastResultsByTaskId();
                markLoaded(CachedTable.FORECAST_RESULT);
            }
        }
    }

    public List<TimeseriesForecastResult> listForecastResults() {
        return List.copyOf(forecastResults);
    }

    public void replaceSyncLogs(Collection<TimeseriesSyncLog> entities) {
        synchronized (syncLogLock) {
            replaceAll(syncLogs, entities);
            markLoaded(CachedTable.SYNC_LOG);
        }
    }

    public void putSyncLog(TimeseriesSyncLog entity) {
        if (entity != null && entity.getId() != null) {
            synchronized (syncLogLock) {
                upsert(syncLogs, item -> entity.getId().equals(item.getId()), entity);
                markLoaded(CachedTable.SYNC_LOG);
            }
        }
    }

    public List<TimeseriesSyncLog> listSyncLogs() {
        return List.copyOf(syncLogs);
    }

    public void replaceTimeseriesDataPoints(Collection<TimeseriesDataPoint> points) {
        synchronized (dataPointLock) {
            replaceAll(dataPoints, points);
            markLoaded(CachedTable.TIMESERIES_DATA);
        }
    }

    public void replaceTimeseriesDataPoints(String sequenceId, Collection<TimeseriesDataPoint> points) {
        synchronized (dataPointLock) {
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
    }

    public void putTimeseriesDataPoints(Collection<TimeseriesDataPoint> points) {
        if (points == null || points.isEmpty()) {
            return;
        }
        synchronized (dataPointLock) {
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
        constraints.forEach(c -> {
            if (c.getConstraintId() != null) constraintByConstraintId.put(c.getConstraintId(), c);
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
