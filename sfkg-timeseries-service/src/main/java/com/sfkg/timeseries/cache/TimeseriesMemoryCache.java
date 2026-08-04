package com.sfkg.timeseries.cache;

import com.sfkg.timeseries.entity.TimeseriesAnomalyTask;
import com.sfkg.timeseries.entity.TimeseriesCategory;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.dto.HistoryDataQueryRequest;
import com.sfkg.timeseries.entity.TimeseriesDataPoint;
import com.sfkg.timeseries.entity.TimeseriesEvent;
import com.sfkg.timeseries.entity.TimeseriesForecastTask;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.entity.TimeseriesRelation;
import com.sfkg.timeseries.entity.TimeseriesSyncLog;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
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
    private final List<TimeseriesSyncLog> syncLogs = new CopyOnWriteArrayList<>();
    private final List<TimeseriesDataPoint> dataPoints = new CopyOnWriteArrayList<>();

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
            case INSTANCE_CONFIG -> instanceConfigs.clear();
            case CATEGORY -> categories.clear();
            case CONSTRAINT -> constraints.clear();
            case RELATION -> relations.clear();
            case EVENT -> events.clear();
            case ANOMALY_TASK -> anomalyTasks.clear();
            case FORECAST_TASK -> forecastTasks.clear();
            case SYNC_LOG -> syncLogs.clear();
            case TIMESERIES_DATA -> dataPoints.clear();
        }
        markUnloaded(table);
    }

    public void replaceInstanceConfigs(Collection<TimeseriesInstanceConfig> entities) {
        replaceAll(instanceConfigs, entities);
        markLoaded(CachedTable.INSTANCE_CONFIG);
    }

    public void putInstanceConfig(TimeseriesInstanceConfig entity) {
        if (entity != null && entity.getSequenceId() != null) {
            upsert(instanceConfigs, item -> entity.getSequenceId().equals(item.getSequenceId()), entity);
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
        markLoaded(CachedTable.CONSTRAINT);
    }

    public void putConstraint(TimeseriesConstraint entity) {
        if (entity != null && entity.getConstraintId() != null) {
            upsert(constraints, item -> entity.getConstraintId().equals(item.getConstraintId()), entity);
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
        markLoaded(CachedTable.RELATION);
    }

    public void putRelation(TimeseriesRelation entity) {
        if (entity != null && entity.getRelationId() != null) {
            upsert(relations, item -> entity.getRelationId().equals(item.getRelationId()), entity);
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
        markLoaded(CachedTable.EVENT);
    }

    public void putEvent(TimeseriesEvent entity) {
        if (entity != null && entity.getEventId() != null) {
            upsert(events, item -> entity.getEventId().equals(item.getEventId()), entity);
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
