package com.sfkg.timeseries.cache;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.sfkg.timeseries.entity.TimeseriesAnomalyResult;
import com.sfkg.timeseries.entity.TimeseriesAnomalyTask;
import com.sfkg.timeseries.entity.TimeseriesCategory;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
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
        return computeInstanceConfig(null, sequenceId, updater);
    }

    public TimeseriesInstanceConfig computeInstanceConfig(String projectId, String sequenceId,
            java.util.function.Function<TimeseriesInstanceConfig, TimeseriesInstanceConfig> updater) {
        synchronized (instanceLock) {
            TimeseriesInstanceConfig existing = getInstanceConfig(projectId, sequenceId).orElse(null);
            TimeseriesInstanceConfig result = updater.apply(existing);
            if (result != null && result.getSequenceId() != null) {
                upsert(instanceConfigs, item -> sameProject(result.getProjectId(), item.getProjectId())
                        && result.getSequenceId().equals(item.getSequenceId()), result);
                instanceBySequenceId.put(cacheKey(result.getProjectId(), result.getSequenceId()), result);
                markLoaded(CachedTable.INSTANCE_CONFIG);
            }
            return result;
        }
    }

    public void putInstanceConfig(TimeseriesInstanceConfig entity) {
        if (entity != null && entity.getSequenceId() != null) {
            synchronized (instanceLock) {
                upsert(instanceConfigs, item -> sameProject(entity.getProjectId(), item.getProjectId())
                        && entity.getSequenceId().equals(item.getSequenceId()), entity);
                instanceBySequenceId.put(cacheKey(entity.getProjectId(), entity.getSequenceId()), entity);
                markLoaded(CachedTable.INSTANCE_CONFIG);
            }
        }
    }

    public Optional<TimeseriesInstanceConfig> getInstanceConfig(String sequenceId) {
        return instanceConfigs.stream()
                .filter(entity -> equalsValue(sequenceId, entity.getSequenceId()))
                .findFirst();
    }

    public Optional<TimeseriesInstanceConfig> getInstanceConfig(String projectId, String sequenceId) {
        return instanceConfigs.stream()
                .filter(entity -> sameProject(projectId, entity.getProjectId())
                        && equalsValue(sequenceId, entity.getSequenceId()))
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
        return computeCategory(null, categoryId, updater);
    }

    public TimeseriesCategory computeCategory(String projectId, String categoryId,
            java.util.function.Function<TimeseriesCategory, TimeseriesCategory> updater) {
        synchronized (categoryLock) {
            TimeseriesCategory existing = getCategory(projectId, categoryId).orElse(null);
            TimeseriesCategory result = updater.apply(existing);
            if (result != null && result.getCategoryId() != null) {
                upsert(categories, item -> sameProject(result.getProjectId(), item.getProjectId())
                        && result.getCategoryId().equals(item.getCategoryId()), result);
                markLoaded(CachedTable.CATEGORY);
            }
            return result;
        }
    }

    public void putCategory(TimeseriesCategory entity) {
        if (entity != null && entity.getCategoryId() != null) {
            synchronized (categoryLock) {
                upsert(categories, item -> sameProject(entity.getProjectId(), item.getProjectId())
                        && entity.getCategoryId().equals(item.getCategoryId()), entity);
                markLoaded(CachedTable.CATEGORY);
            }
        }
    }

    public Optional<TimeseriesCategory> getCategory(String categoryId) {
        return categories.stream()
                .filter(entity -> equalsValue(categoryId, entity.getCategoryId()))
                .findFirst();
    }

    public Optional<TimeseriesCategory> getCategory(String projectId, String categoryId) {
        return categories.stream()
                .filter(entity -> sameProject(projectId, entity.getProjectId())
                        && equalsValue(categoryId, entity.getCategoryId()))
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
        return computeConstraint(null, constraintId, updater);
    }

    public TimeseriesConstraint computeConstraint(String projectId, String constraintId,
            java.util.function.Function<TimeseriesConstraint, TimeseriesConstraint> updater) {
        synchronized (constraintLock) {
            TimeseriesConstraint existing = getConstraint(projectId, constraintId).orElse(null);
            TimeseriesConstraint result = updater.apply(existing);
            if (result != null && result.getConstraintId() != null) {
                upsert(constraints, item -> sameProject(result.getProjectId(), item.getProjectId())
                        && result.getConstraintId().equals(item.getConstraintId()), result);
                constraintByConstraintId.put(cacheKey(result.getProjectId(), result.getConstraintId()), result);
                markLoaded(CachedTable.CONSTRAINT);
            }
            return result;
        }
    }

    public void putConstraint(TimeseriesConstraint entity) {
        if (entity != null && entity.getConstraintId() != null) {
            synchronized (constraintLock) {
                upsert(constraints, item -> sameProject(entity.getProjectId(), item.getProjectId())
                        && entity.getConstraintId().equals(item.getConstraintId()), entity);
                constraintByConstraintId.put(cacheKey(entity.getProjectId(), entity.getConstraintId()), entity);
                markLoaded(CachedTable.CONSTRAINT);
            }
        }
    }

    public Optional<TimeseriesConstraint> getConstraint(String constraintId) {
        return constraints.stream()
                .filter(entity -> equalsValue(constraintId, entity.getConstraintId()))
                .findFirst();
    }

    public Optional<TimeseriesConstraint> getConstraint(String projectId, String constraintId) {
        return constraints.stream()
                .filter(entity -> sameProject(projectId, entity.getProjectId())
                        && equalsValue(constraintId, entity.getConstraintId()))
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
        return computeRelation(null, relationId, updater);
    }

    public TimeseriesRelation computeRelation(String projectId, String relationId,
            java.util.function.Function<TimeseriesRelation, TimeseriesRelation> updater) {
        synchronized (relationLock) {
            TimeseriesRelation existing = getRelation(projectId, relationId).orElse(null);
            TimeseriesRelation result = updater.apply(existing);
            if (result != null && result.getRelationId() != null) {
                upsert(relations, item -> sameProject(result.getProjectId(), item.getProjectId())
                        && result.getRelationId().equals(item.getRelationId()), result);
                relationByRelationId.put(cacheKey(result.getProjectId(), result.getRelationId()), result);
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
                upsert(relations, item -> sameProject(entity.getProjectId(), item.getProjectId())
                        && entity.getRelationId().equals(item.getRelationId()), entity);
                relationByRelationId.put(cacheKey(entity.getProjectId(), entity.getRelationId()), entity);
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

    public Optional<TimeseriesRelation> getRelation(String projectId, String relationId) {
        return relations.stream()
                .filter(entity -> sameProject(projectId, entity.getProjectId())
                        && equalsValue(relationId, entity.getRelationId()))
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
                entities.forEach(e -> { if (e.getEventId() != null) eventsByEventId.put(cacheKey(e.getProjectId(), e.getEventId()), e); });
            }
            markLoaded(CachedTable.EVENT);
        }
    }

    public TimeseriesEvent computeEvent(String eventId,
            java.util.function.Function<TimeseriesEvent, TimeseriesEvent> updater) {
        return computeEvent(null, eventId, updater);
    }

    public TimeseriesEvent computeEvent(String projectId, String eventId,
            java.util.function.Function<TimeseriesEvent, TimeseriesEvent> updater) {
        synchronized (eventLock) {
            TimeseriesEvent existing = getEvent(projectId, eventId).orElse(null);
            TimeseriesEvent result = updater.apply(existing);
            if (result != null && result.getEventId() != null) {
                upsert(events, item -> sameProject(result.getProjectId(), item.getProjectId())
                        && result.getEventId().equals(item.getEventId()), result);
                eventsByEventId.put(cacheKey(result.getProjectId(), result.getEventId()), result);
                markLoaded(CachedTable.EVENT);
            }
            return result;
        }
    }

    public void putEvent(TimeseriesEvent entity) {
        if (entity != null && entity.getEventId() != null) {
            synchronized (eventLock) {
                upsert(events, item -> sameProject(entity.getProjectId(), item.getProjectId())
                        && entity.getEventId().equals(item.getEventId()), entity);
                eventsByEventId.put(cacheKey(entity.getProjectId(), entity.getEventId()), entity);
                markLoaded(CachedTable.EVENT);
            }
        }
    }

    public Optional<TimeseriesEvent> getEvent(String eventId) {
        return events.stream()
                .filter(entity -> equalsValue(eventId, entity.getEventId()))
                .findFirst();
    }

    public Optional<TimeseriesEvent> getEvent(String projectId, String eventId) {
        return events.stream()
                .filter(entity -> sameProject(projectId, entity.getProjectId())
                        && equalsValue(eventId, entity.getEventId()))
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
        return computeAnomalyTask(null, taskId, updater);
    }

    public TimeseriesAnomalyTask computeAnomalyTask(String projectId, String taskId,
            java.util.function.Function<TimeseriesAnomalyTask, TimeseriesAnomalyTask> updater) {
        synchronized (anomalyTaskLock) {
            TimeseriesAnomalyTask existing = getAnomalyTask(projectId, taskId).orElse(null);
            TimeseriesAnomalyTask result = updater.apply(existing);
            if (result != null && result.getTaskId() != null) {
                upsert(anomalyTasks, item -> sameProject(result.getProjectId(), item.getProjectId())
                        && result.getTaskId().equals(item.getTaskId()), result);
                markLoaded(CachedTable.ANOMALY_TASK);
            }
            return result;
        }
    }

    public void putAnomalyTask(TimeseriesAnomalyTask entity) {
        if (entity != null && entity.getTaskId() != null) {
            synchronized (anomalyTaskLock) {
                upsert(anomalyTasks, item -> sameProject(entity.getProjectId(), item.getProjectId())
                        && entity.getTaskId().equals(item.getTaskId()), entity);
                markLoaded(CachedTable.ANOMALY_TASK);
            }
        }
    }

    public Optional<TimeseriesAnomalyTask> getAnomalyTask(String taskId) {
        return anomalyTasks.stream()
                .filter(entity -> equalsValue(taskId, entity.getTaskId()))
                .findFirst();
    }

    public Optional<TimeseriesAnomalyTask> getAnomalyTask(String projectId, String taskId) {
        return anomalyTasks.stream()
                .filter(entity -> sameProject(projectId, entity.getProjectId())
                        && equalsValue(taskId, entity.getTaskId()))
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
        return computeForecastTask(null, taskId, updater);
    }

    public TimeseriesForecastTask computeForecastTask(String projectId, String taskId,
            java.util.function.Function<TimeseriesForecastTask, TimeseriesForecastTask> updater) {
        synchronized (forecastTaskLock) {
            TimeseriesForecastTask existing = getForecastTask(projectId, taskId).orElse(null);
            TimeseriesForecastTask result = updater.apply(existing);
            if (result != null && result.getTaskId() != null) {
                upsert(forecastTasks, item -> sameProject(result.getProjectId(), item.getProjectId())
                        && result.getTaskId().equals(item.getTaskId()), result);
                markLoaded(CachedTable.FORECAST_TASK);
            }
            return result;
        }
    }

    public void putForecastTask(TimeseriesForecastTask entity) {
        if (entity != null && entity.getTaskId() != null) {
            synchronized (forecastTaskLock) {
                upsert(forecastTasks, item -> sameProject(entity.getProjectId(), item.getProjectId())
                        && entity.getTaskId().equals(item.getTaskId()), entity);
                markLoaded(CachedTable.FORECAST_TASK);
            }
        }
    }

    public Optional<TimeseriesForecastTask> getForecastTask(String taskId) {
        return forecastTasks.stream()
                .filter(entity -> equalsValue(taskId, entity.getTaskId()))
                .findFirst();
    }

    public Optional<TimeseriesForecastTask> getForecastTask(String projectId, String taskId) {
        return forecastTasks.stream()
                .filter(entity -> sameProject(projectId, entity.getProjectId())
                        && equalsValue(taskId, entity.getTaskId()))
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
                upsert(anomalyResults, item -> sameProject(entity.getProjectId(), item.getProjectId())
                        && entity.getResultId().equals(item.getResultId()), entity);
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
                upsert(forecastResults, item -> sameProject(entity.getProjectId(), item.getProjectId())
                        && entity.getResultId().equals(item.getResultId()), entity);
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
                upsert(syncLogs, item -> sameProject(entity.getProjectId(), item.getProjectId())
                        && entity.getId().equals(item.getId()), entity);
                markLoaded(CachedTable.SYNC_LOG);
            }
        }
    }

    public List<TimeseriesSyncLog> listSyncLogs() {
        return List.copyOf(syncLogs);
    }

    // ── Indexed query methods ────────────────────────────────────────

    public TimeseriesInstanceConfig getInstanceBySequenceId(String sequenceId) {
        return sequenceId == null ? null : instanceConfigs.stream()
                .filter(item -> sequenceId.equals(item.getSequenceId()))
                .findFirst().orElse(null);
    }

    public TimeseriesInstanceConfig getInstanceBySequenceId(String projectId, String sequenceId) {
        return sequenceId != null ? instanceBySequenceId.get(cacheKey(projectId, sequenceId)) : null;
    }

    public TimeseriesRelation getRelationByRelationId(String relationId) {
        return relationId == null ? null : relations.stream()
                .filter(item -> relationId.equals(item.getRelationId()))
                .findFirst().orElse(null);
    }

    public TimeseriesRelation getRelationByRelationId(String projectId, String relationId) {
        return relationId != null ? relationByRelationId.get(cacheKey(projectId, relationId)) : null;
    }

    public List<TimeseriesRelation> listRelationsByTargetSequenceId(String sequenceId) {
        return relations.stream().filter(item -> sequenceId != null
                && sequenceId.equals(item.getTargetSequenceId())).toList();
    }

    public List<TimeseriesRelation> listRelationsByTargetSequenceId(String projectId, String sequenceId) {
        return sequenceId != null && relationsByTargetSequenceId.containsKey(cacheKey(projectId, sequenceId))
                ? List.copyOf(relationsByTargetSequenceId.get(cacheKey(projectId, sequenceId)))
                : List.of();
    }

    public List<TimeseriesRelation> listRelationsBySourceSequenceId(String sequenceId) {
        return relations.stream().filter(item -> sequenceId != null
                && item.getSourceSequences() != null && item.getSourceSequences().contains(sequenceId)).toList();
    }

    public List<TimeseriesRelation> listRelationsBySourceSequenceId(String projectId, String sequenceId) {
        return sequenceId != null && relationsBySourceSequenceId.containsKey(cacheKey(projectId, sequenceId))
                ? List.copyOf(relationsBySourceSequenceId.get(cacheKey(projectId, sequenceId)))
                : List.of();
    }

    public TimeseriesEvent getEventByEventId(String eventId) {
        return eventId == null ? null : events.stream()
                .filter(item -> eventId.equals(item.getEventId()))
                .findFirst().orElse(null);
    }

    public TimeseriesEvent getEventByEventId(String projectId, String eventId) {
        return eventId != null ? eventsByEventId.get(cacheKey(projectId, eventId)) : null;
    }

    public List<TimeseriesAnomalyResult> listAnomalyResultsByTaskId(String taskId) {
        return anomalyResults.stream().filter(item -> taskId != null
                && ((item.getTaskId() != null && taskId.equals(item.getTaskId()))
                || (item.getSource() != null && taskId.equals(item.getSource())))).toList();
    }

    public List<TimeseriesAnomalyResult> listAnomalyResultsByTaskId(String projectId, String taskId) {
        return taskId != null && anomalyResultsByTaskId.containsKey(cacheKey(projectId, taskId))
                ? List.copyOf(anomalyResultsByTaskId.get(cacheKey(projectId, taskId)))
                : List.of();
    }

    public List<TimeseriesForecastResult> listForecastResultsByTaskId(String taskId) {
        return forecastResults.stream().filter(item -> taskId != null && taskId.equals(item.getTaskId())).toList();
    }

    public List<TimeseriesForecastResult> listForecastResultsByTaskId(String projectId, String taskId) {
        return taskId != null && forecastResultsByTaskId.containsKey(cacheKey(projectId, taskId))
                ? List.copyOf(forecastResultsByTaskId.get(cacheKey(projectId, taskId)))
                : List.of();
    }

    // ── Internal index rebuild helpers ────────────────────────────────

    private void rebuildInstanceIndex() {
        instanceBySequenceId.clear();
        instanceConfigs.forEach(c -> {
            if (c.getSequenceId() != null) instanceBySequenceId.put(cacheKey(c.getProjectId(), c.getSequenceId()), c);
        });
    }

    private void rebuildConstraintIndexes() {
        constraintByConstraintId.clear();
        constraints.forEach(c -> {
            if (c.getConstraintId() != null) constraintByConstraintId.put(cacheKey(c.getProjectId(), c.getConstraintId()), c);
        });
    }

    private void rebuildRelationIndexes() {
        relationByRelationId.clear();
        relations.forEach(r -> {
            if (r.getRelationId() != null) relationByRelationId.put(cacheKey(r.getProjectId(), r.getRelationId()), r);
        });
        rebuildRelationsByTargetId();
        rebuildRelationsBySourceId();
    }

    private void rebuildRelationsByTargetId() {
        relationsByTargetSequenceId.clear();
        relations.forEach(r -> {
            if (r.getTargetSequenceId() != null) {
                relationsByTargetSequenceId.computeIfAbsent(cacheKey(r.getProjectId(), r.getTargetSequenceId()), k -> new CopyOnWriteArrayList<>()).add(r);
            }
        });
    }

    private void rebuildRelationsBySourceId() {
        relationsBySourceSequenceId.clear();
        relations.forEach(r -> {
            if (r.getSourceSequences() != null) {
                r.getSourceSequences().forEach(src -> {
                    if (src != null) {
                        relationsBySourceSequenceId.computeIfAbsent(cacheKey(r.getProjectId(), src), k -> new CopyOnWriteArrayList<>()).add(r);
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
                anomalyResultsByTaskId.computeIfAbsent(cacheKey(r.getProjectId(), r.getSource()), k -> new CopyOnWriteArrayList<>()).add(r);
            }
            if (r.getSequenceIds() != null) {
                r.getSequenceIds().forEach(sid -> {
                    if (sid != null) {
                        anomalyResultsByTaskId.computeIfAbsent(cacheKey(r.getProjectId(), sid), k -> new CopyOnWriteArrayList<>()).add(r);
                    }
                });
            }
        });
    }

    private void rebuildForecastResultsByTaskId() {
        forecastResultsByTaskId.clear();
        forecastResults.forEach(r -> {
            if (r.getTaskId() != null) {
                forecastResultsByTaskId.computeIfAbsent(cacheKey(r.getProjectId(), r.getTaskId()), k -> new CopyOnWriteArrayList<>()).add(r);
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

    private boolean sameProject(String left, String right) {
        return java.util.Objects.equals(left, right);
    }

    private String cacheKey(String projectId, String id) {
        return String.valueOf(projectId) + "::" + String.valueOf(id);
    }
}
