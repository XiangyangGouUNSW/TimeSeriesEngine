package com.sfkg.timeseries.cache;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.function.Function;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger LOG = LoggerFactory.getLogger(TimeseriesMemoryCache.class);

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
    private final Map<String, ProjectCacheBucket> projectBuckets = new ConcurrentHashMap<>();

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
            case INSTANCE_CONFIG -> { instanceConfigs.clear(); clearProjectTable(bucket -> bucket.instanceConfigs.clear()); instanceBySequenceId.clear(); }
            case CATEGORY -> { categories.clear(); clearProjectTable(bucket -> bucket.categories.clear()); }
            case CONSTRAINT -> { constraints.clear(); clearProjectTable(bucket -> bucket.constraints.clear()); constraintByConstraintId.clear(); }
            case RELATION -> { relations.clear(); clearProjectTable(bucket -> bucket.relations.clear()); relationByRelationId.clear(); relationsByTargetSequenceId.clear(); relationsBySourceSequenceId.clear(); }
            case EVENT -> { events.clear(); clearProjectTable(bucket -> bucket.events.clear()); eventsByEventId.clear(); }
            case ANOMALY_TASK -> { anomalyTasks.clear(); clearProjectTable(bucket -> bucket.anomalyTasks.clear()); }
            case FORECAST_TASK -> { forecastTasks.clear(); clearProjectTable(bucket -> bucket.forecastTasks.clear()); }
            case ANOMALY_RESULT -> { anomalyResults.clear(); clearProjectTable(bucket -> bucket.anomalyResults.clear()); anomalyResultsByTaskId.clear(); }
            case FORECAST_RESULT -> { forecastResults.clear(); clearProjectTable(bucket -> bucket.forecastResults.clear()); forecastResultsByTaskId.clear(); }
            case SYNC_LOG -> { syncLogs.clear(); clearProjectTable(bucket -> bucket.syncLogs.clear()); }
        }
        markUnloaded(table);
    }

    public void replaceInstanceConfigs(Collection<TimeseriesInstanceConfig> entities) {
        synchronized (instanceLock) {
            replaceAll(instanceConfigs, entities);
            rebuildProjectTable(entities, bucket -> bucket.instanceConfigs,
                    bucket -> bucket.instanceConfigs.clear(), TimeseriesInstanceConfig::getProjectId);
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
                String oldKey = cacheKey(projectId, sequenceId);
                String newKey = cacheKey(result.getProjectId(), result.getSequenceId());
                if (!oldKey.equals(newKey)) {
                    instanceBySequenceId.remove(oldKey);
                }
                upsert(instanceConfigs, item -> sameProject(result.getProjectId(), item.getProjectId())
                        && result.getSequenceId().equals(item.getSequenceId()), result);
                upsertProjectBucket(result, bucket -> bucket.instanceConfigs,
                        item -> sameProject(result.getProjectId(), item.getProjectId())
                                && result.getSequenceId().equals(item.getSequenceId()));
                instanceBySequenceId.put(newKey, result);
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
                upsertProjectBucket(entity, bucket -> bucket.instanceConfigs,
                        item -> sameProject(entity.getProjectId(), item.getProjectId())
                                && entity.getSequenceId().equals(item.getSequenceId()));
                instanceBySequenceId.put(cacheKey(entity.getProjectId(), entity.getSequenceId()), entity);
                markLoaded(CachedTable.INSTANCE_CONFIG);
            }
        }
    }

    /**
     * @deprecated use {@link #getInstanceConfig(String, String)} with an
     * explicit projectId — without it the match is ambiguous across projects.
     */
    @Deprecated
    public Optional<TimeseriesInstanceConfig> getInstanceConfig(String sequenceId) {
        return firstAcrossProjects("getInstanceConfig",
                instanceConfigs.stream()
                        .filter(entity -> equalsValue(sequenceId, entity.getSequenceId()))
                        .collect(Collectors.toList()),
                TimeseriesInstanceConfig::getProjectId);
    }

    public Optional<TimeseriesInstanceConfig> getInstanceConfig(String projectId, String sequenceId) {
        if (sequenceId == null) {
            return Optional.empty();
        }
        TimeseriesInstanceConfig found = instanceBySequenceId.get(cacheKey(projectId, sequenceId));
        return found != null
                && sequenceId.equals(found.getSequenceId())
                && sameProject(projectId, found.getProjectId())
                ? Optional.of(found)
                : Optional.empty();
    }

    public List<TimeseriesInstanceConfig> listInstanceConfigs() {
        return List.copyOf(instanceConfigs);
    }

    public List<TimeseriesInstanceConfig> listInstanceConfigs(String projectId) {
        return projectBucketContents(projectId, bucket -> bucket.instanceConfigs);
    }

    public void replaceCategories(Collection<TimeseriesCategory> entities) {
        synchronized (categoryLock) {
            replaceAll(categories, entities);
            rebuildProjectTable(entities, bucket -> bucket.categories,
                    bucket -> bucket.categories.clear(), TimeseriesCategory::getProjectId);
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
                upsertProjectBucket(result, bucket -> bucket.categories,
                        item -> sameProject(result.getProjectId(), item.getProjectId())
                                && result.getCategoryId().equals(item.getCategoryId()));
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
                upsertProjectBucket(entity, bucket -> bucket.categories,
                        item -> sameProject(entity.getProjectId(), item.getProjectId())
                                && entity.getCategoryId().equals(item.getCategoryId()));
                markLoaded(CachedTable.CATEGORY);
            }
        }
    }

    /**
     * @deprecated use {@link #getCategory(String, String)} with an explicit projectId.
     */
    @Deprecated
    public Optional<TimeseriesCategory> getCategory(String categoryId) {
        return firstAcrossProjects("getCategory",
                categories.stream()
                        .filter(entity -> equalsValue(categoryId, entity.getCategoryId()))
                        .collect(Collectors.toList()),
                TimeseriesCategory::getProjectId);
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

    public List<TimeseriesCategory> listCategories(String projectId) {
        return projectBucketContents(projectId, bucket -> bucket.categories);
    }

    public void replaceConstraints(Collection<TimeseriesConstraint> entities) {
        synchronized (constraintLock) {
            replaceAll(constraints, entities);
            rebuildProjectTable(entities, bucket -> bucket.constraints,
                    bucket -> bucket.constraints.clear(), TimeseriesConstraint::getProjectId);
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
                String oldKey = cacheKey(projectId, constraintId);
                String newKey = cacheKey(result.getProjectId(), result.getConstraintId());
                if (!oldKey.equals(newKey)) {
                    constraintByConstraintId.remove(oldKey);
                }
                upsert(constraints, item -> sameProject(result.getProjectId(), item.getProjectId())
                        && result.getConstraintId().equals(item.getConstraintId()), result);
                upsertProjectBucket(result, bucket -> bucket.constraints,
                        item -> sameProject(result.getProjectId(), item.getProjectId())
                                && result.getConstraintId().equals(item.getConstraintId()));
                constraintByConstraintId.put(newKey, result);
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
                upsertProjectBucket(entity, bucket -> bucket.constraints,
                        item -> sameProject(entity.getProjectId(), item.getProjectId())
                                && entity.getConstraintId().equals(item.getConstraintId()));
                constraintByConstraintId.put(cacheKey(entity.getProjectId(), entity.getConstraintId()), entity);
                markLoaded(CachedTable.CONSTRAINT);
            }
        }
    }

    /**
     * @deprecated use {@link #getConstraint(String, String)} with an explicit projectId.
     */
    @Deprecated
    public Optional<TimeseriesConstraint> getConstraint(String constraintId) {
        return firstAcrossProjects("getConstraint",
                constraints.stream()
                        .filter(entity -> equalsValue(constraintId, entity.getConstraintId()))
                        .collect(Collectors.toList()),
                TimeseriesConstraint::getProjectId);
    }

    public Optional<TimeseriesConstraint> getConstraint(String projectId, String constraintId) {
        if (constraintId == null) {
            return Optional.empty();
        }
        TimeseriesConstraint found = constraintByConstraintId.get(cacheKey(projectId, constraintId));
        return found != null
                && constraintId.equals(found.getConstraintId())
                && sameProject(projectId, found.getProjectId())
                ? Optional.of(found)
                : Optional.empty();
    }

    public List<TimeseriesConstraint> listConstraints() {
        return List.copyOf(constraints);
    }

    public List<TimeseriesConstraint> listConstraints(String projectId) {
        return projectBucketContents(projectId, bucket -> bucket.constraints);
    }

    public void replaceRelations(Collection<TimeseriesRelation> entities) {
        synchronized (relationLock) {
            replaceAll(relations, entities);
            rebuildProjectTable(entities, bucket -> bucket.relations,
                    bucket -> bucket.relations.clear(), TimeseriesRelation::getProjectId);
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
                removeRelationFromIndexes(existing);
                String oldKey = cacheKey(projectId, relationId);
                String newKey = cacheKey(result.getProjectId(), result.getRelationId());
                if (!oldKey.equals(newKey)) {
                    relationByRelationId.remove(oldKey);
                }
                upsert(relations, item -> sameProject(result.getProjectId(), item.getProjectId())
                        && result.getRelationId().equals(item.getRelationId()), result);
                upsertProjectBucket(result, bucket -> bucket.relations,
                        item -> sameProject(result.getProjectId(), item.getProjectId())
                                && result.getRelationId().equals(item.getRelationId()));
                relationByRelationId.put(newKey, result);
                addRelationToIndexes(result);
                markLoaded(CachedTable.RELATION);
            }
            return result;
        }
    }

    public void putRelation(TimeseriesRelation entity) {
        if (entity != null && entity.getRelationId() != null) {
            synchronized (relationLock) {
                TimeseriesRelation existing = getRelation(entity.getProjectId(), entity.getRelationId()).orElse(null);
                removeRelationFromIndexes(existing);
                upsert(relations, item -> sameProject(entity.getProjectId(), item.getProjectId())
                        && entity.getRelationId().equals(item.getRelationId()), entity);
                upsertProjectBucket(entity, bucket -> bucket.relations,
                        item -> sameProject(entity.getProjectId(), item.getProjectId())
                                && entity.getRelationId().equals(item.getRelationId()));
                relationByRelationId.put(cacheKey(entity.getProjectId(), entity.getRelationId()), entity);
                addRelationToIndexes(entity);
                markLoaded(CachedTable.RELATION);
            }
        }
    }

    /**
     * @deprecated use {@link #getRelation(String, String)} with an explicit projectId.
     */
    @Deprecated
    public Optional<TimeseriesRelation> getRelation(String relationId) {
        return firstAcrossProjects("getRelation",
                relations.stream()
                        .filter(entity -> equalsValue(relationId, entity.getRelationId()))
                        .collect(Collectors.toList()),
                TimeseriesRelation::getProjectId);
    }

    public Optional<TimeseriesRelation> getRelation(String projectId, String relationId) {
        if (relationId == null) {
            return Optional.empty();
        }
        TimeseriesRelation found = relationByRelationId.get(cacheKey(projectId, relationId));
        return found != null
                && relationId.equals(found.getRelationId())
                && sameProject(projectId, found.getProjectId())
                ? Optional.of(found)
                : Optional.empty();
    }

    public List<TimeseriesRelation> listRelations() {
        return List.copyOf(relations);
    }

    public List<TimeseriesRelation> listRelations(String projectId) {
        return projectBucketContents(projectId, bucket -> bucket.relations);
    }

    public void replaceEvents(Collection<TimeseriesEvent> entities) {
        synchronized (eventLock) {
            replaceAll(events, entities);
            rebuildProjectTable(entities, bucket -> bucket.events,
                    bucket -> bucket.events.clear(), TimeseriesEvent::getProjectId);
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
                String oldKey = cacheKey(projectId, eventId);
                String newKey = cacheKey(result.getProjectId(), result.getEventId());
                if (!oldKey.equals(newKey)) {
                    eventsByEventId.remove(oldKey);
                }
                upsert(events, item -> sameProject(result.getProjectId(), item.getProjectId())
                        && result.getEventId().equals(item.getEventId()), result);
                upsertProjectBucket(result, bucket -> bucket.events,
                        item -> sameProject(result.getProjectId(), item.getProjectId())
                                && result.getEventId().equals(item.getEventId()));
                eventsByEventId.put(newKey, result);
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
                upsertProjectBucket(entity, bucket -> bucket.events,
                        item -> sameProject(entity.getProjectId(), item.getProjectId())
                                && entity.getEventId().equals(item.getEventId()));
                eventsByEventId.put(cacheKey(entity.getProjectId(), entity.getEventId()), entity);
                markLoaded(CachedTable.EVENT);
            }
        }
    }

    /**
     * @deprecated use {@link #getEvent(String, String)} with an explicit projectId.
     */
    @Deprecated
    public Optional<TimeseriesEvent> getEvent(String eventId) {
        return firstAcrossProjects("getEvent",
                events.stream()
                        .filter(entity -> equalsValue(eventId, entity.getEventId()))
                        .collect(Collectors.toList()),
                TimeseriesEvent::getProjectId);
    }

    public Optional<TimeseriesEvent> getEvent(String projectId, String eventId) {
        if (eventId == null) {
            return Optional.empty();
        }
        TimeseriesEvent found = eventsByEventId.get(cacheKey(projectId, eventId));
        return found != null
                && eventId.equals(found.getEventId())
                && sameProject(projectId, found.getProjectId())
                ? Optional.of(found)
                : Optional.empty();
    }

    public List<TimeseriesEvent> listEvents() {
        return List.copyOf(events);
    }

    public List<TimeseriesEvent> listEvents(String projectId) {
        return projectBucketContents(projectId, bucket -> bucket.events);
    }

    public void replaceAnomalyTasks(Collection<TimeseriesAnomalyTask> entities) {
        synchronized (anomalyTaskLock) {
            replaceAll(anomalyTasks, entities);
            rebuildProjectTable(entities, bucket -> bucket.anomalyTasks,
                    bucket -> bucket.anomalyTasks.clear(), TimeseriesAnomalyTask::getProjectId);
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
                upsertProjectBucket(result, bucket -> bucket.anomalyTasks,
                        item -> sameProject(result.getProjectId(), item.getProjectId())
                                && result.getTaskId().equals(item.getTaskId()));
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
                upsertProjectBucket(entity, bucket -> bucket.anomalyTasks,
                        item -> sameProject(entity.getProjectId(), item.getProjectId())
                                && entity.getTaskId().equals(item.getTaskId()));
                markLoaded(CachedTable.ANOMALY_TASK);
            }
        }
    }

    /**
     * @deprecated use {@link #getAnomalyTask(String, String)} with an explicit projectId.
     */
    @Deprecated
    public Optional<TimeseriesAnomalyTask> getAnomalyTask(String taskId) {
        return firstAcrossProjects("getAnomalyTask",
                anomalyTasks.stream()
                        .filter(entity -> equalsValue(taskId, entity.getTaskId()))
                        .collect(Collectors.toList()),
                TimeseriesAnomalyTask::getProjectId);
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

    public List<TimeseriesAnomalyTask> listAnomalyTasks(String projectId) {
        return projectBucketContents(projectId, bucket -> bucket.anomalyTasks);
    }

    public void replaceForecastTasks(Collection<TimeseriesForecastTask> entities) {
        synchronized (forecastTaskLock) {
            replaceAll(forecastTasks, entities);
            rebuildProjectTable(entities, bucket -> bucket.forecastTasks,
                    bucket -> bucket.forecastTasks.clear(), TimeseriesForecastTask::getProjectId);
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
                upsertProjectBucket(result, bucket -> bucket.forecastTasks,
                        item -> sameProject(result.getProjectId(), item.getProjectId())
                                && result.getTaskId().equals(item.getTaskId()));
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
                upsertProjectBucket(entity, bucket -> bucket.forecastTasks,
                        item -> sameProject(entity.getProjectId(), item.getProjectId())
                                && entity.getTaskId().equals(item.getTaskId()));
                markLoaded(CachedTable.FORECAST_TASK);
            }
        }
    }

    /**
     * @deprecated use {@link #getForecastTask(String, String)} with an explicit projectId.
     */
    @Deprecated
    public Optional<TimeseriesForecastTask> getForecastTask(String taskId) {
        return firstAcrossProjects("getForecastTask",
                forecastTasks.stream()
                        .filter(entity -> equalsValue(taskId, entity.getTaskId()))
                        .collect(Collectors.toList()),
                TimeseriesForecastTask::getProjectId);
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

    public List<TimeseriesForecastTask> listForecastTasks(String projectId) {
        return projectBucketContents(projectId, bucket -> bucket.forecastTasks);
    }

    public void replaceAnomalyResults(Collection<TimeseriesAnomalyResult> entities) {
        synchronized (anomalyResultLock) {
            replaceAll(anomalyResults, entities);
            rebuildProjectTable(entities, bucket -> bucket.anomalyResults,
                    bucket -> bucket.anomalyResults.clear(), TimeseriesAnomalyResult::getProjectId);
            rebuildAnomalyResultsByTaskId();
            markLoaded(CachedTable.ANOMALY_RESULT);
        }
    }

    public void putAnomalyResult(TimeseriesAnomalyResult entity) {
        if (entity != null && entity.getResultId() != null) {
            synchronized (anomalyResultLock) {
                upsert(anomalyResults, item -> sameProject(entity.getProjectId(), item.getProjectId())
                        && entity.getResultId().equals(item.getResultId()), entity);
                upsertProjectBucket(entity, bucket -> bucket.anomalyResults,
                        item -> sameProject(entity.getProjectId(), item.getProjectId())
                                && entity.getResultId().equals(item.getResultId()));
                rebuildAnomalyResultsByTaskId();
                markLoaded(CachedTable.ANOMALY_RESULT);
            }
        }
    }

    public List<TimeseriesAnomalyResult> listAnomalyResults() {
        return List.copyOf(anomalyResults);
    }

    public List<TimeseriesAnomalyResult> listAnomalyResults(String projectId) {
        return projectBucketContents(projectId, bucket -> bucket.anomalyResults);
    }

    public void replaceForecastResults(Collection<TimeseriesForecastResult> entities) {
        synchronized (forecastResultLock) {
            replaceAll(forecastResults, entities);
            rebuildProjectTable(entities, bucket -> bucket.forecastResults,
                    bucket -> bucket.forecastResults.clear(), TimeseriesForecastResult::getProjectId);
            rebuildForecastResultsByTaskId();
            markLoaded(CachedTable.FORECAST_RESULT);
        }
    }

    public void putForecastResult(TimeseriesForecastResult entity) {
        if (entity != null && entity.getResultId() != null) {
            synchronized (forecastResultLock) {
                upsert(forecastResults, item -> sameProject(entity.getProjectId(), item.getProjectId())
                        && entity.getResultId().equals(item.getResultId()), entity);
                upsertProjectBucket(entity, bucket -> bucket.forecastResults,
                        item -> sameProject(entity.getProjectId(), item.getProjectId())
                                && entity.getResultId().equals(item.getResultId()));
                rebuildForecastResultsByTaskId();
                markLoaded(CachedTable.FORECAST_RESULT);
            }
        }
    }

    public List<TimeseriesForecastResult> listForecastResults() {
        return List.copyOf(forecastResults);
    }

    public List<TimeseriesForecastResult> listForecastResults(String projectId) {
        return projectBucketContents(projectId, bucket -> bucket.forecastResults);
    }

    public void replaceSyncLogs(Collection<TimeseriesSyncLog> entities) {
        synchronized (syncLogLock) {
            replaceAll(syncLogs, entities);
            rebuildProjectTable(entities, bucket -> bucket.syncLogs,
                    bucket -> bucket.syncLogs.clear(), TimeseriesSyncLog::getProjectId);
            markLoaded(CachedTable.SYNC_LOG);
        }
    }

    public void putSyncLog(TimeseriesSyncLog entity) {
        if (entity != null && entity.getId() != null) {
            synchronized (syncLogLock) {
                upsert(syncLogs, item -> sameProject(entity.getProjectId(), item.getProjectId())
                        && entity.getId().equals(item.getId()), entity);
                upsertProjectBucket(entity, bucket -> bucket.syncLogs,
                        item -> sameProject(entity.getProjectId(), item.getProjectId())
                                && entity.getId().equals(item.getId()));
                markLoaded(CachedTable.SYNC_LOG);
            }
        }
    }

    public List<TimeseriesSyncLog> listSyncLogs() {
        return List.copyOf(syncLogs);
    }

    public List<TimeseriesSyncLog> listSyncLogs(String projectId) {
        return projectBucketContents(projectId, bucket -> bucket.syncLogs);
    }

    // ── Indexed query methods ────────────────────────────────────────

    /**
     * @deprecated use {@link #getInstanceBySequenceId(String, String)} with an
     * explicit projectId — without it the match is ambiguous across projects.
     */
    @Deprecated
    public TimeseriesInstanceConfig getInstanceBySequenceId(String sequenceId) {
        return firstAcrossProjects("getInstanceBySequenceId",
                sequenceId == null
                        ? List.of()
                        : instanceConfigs.stream()
                                .filter(item -> sequenceId.equals(item.getSequenceId()))
                                .collect(Collectors.toList()),
                TimeseriesInstanceConfig::getProjectId)
                .orElse(null);
    }

    public TimeseriesInstanceConfig getInstanceBySequenceId(String projectId, String sequenceId) {
        if (sequenceId == null) {
            return null;
        }
        TimeseriesInstanceConfig found = instanceBySequenceId.get(cacheKey(projectId, sequenceId));
        return found != null
                && sequenceId.equals(found.getSequenceId())
                && sameProject(projectId, found.getProjectId())
                ? found : null;
    }

    /**
     * @deprecated use {@link #getRelationByRelationId(String, String)} with an explicit projectId.
     */
    @Deprecated
    public TimeseriesRelation getRelationByRelationId(String relationId) {
        return firstAcrossProjects("getRelationByRelationId",
                relationId == null
                        ? List.of()
                        : relations.stream()
                                .filter(item -> relationId.equals(item.getRelationId()))
                                .collect(Collectors.toList()),
                TimeseriesRelation::getProjectId)
                .orElse(null);
    }

    public TimeseriesRelation getRelationByRelationId(String projectId, String relationId) {
        if (relationId == null) {
            return null;
        }
        TimeseriesRelation found = relationByRelationId.get(cacheKey(projectId, relationId));
        return found != null
                && relationId.equals(found.getRelationId())
                && sameProject(projectId, found.getProjectId())
                ? found : null;
    }

    /**
     * @deprecated use {@link #listRelationsByTargetSequenceId(String, String)} with an explicit projectId.
     */
    @Deprecated
    public List<TimeseriesRelation> listRelationsByTargetSequenceId(String sequenceId) {
        return relations.stream().filter(item -> sequenceId != null
                && sequenceId.equals(item.getTargetSequenceId())).toList();
    }

    public List<TimeseriesRelation> listRelationsByTargetSequenceId(String projectId, String sequenceId) {
        return sequenceId != null && relationsByTargetSequenceId.containsKey(cacheKey(projectId, sequenceId))
                ? List.copyOf(relationsByTargetSequenceId.get(cacheKey(projectId, sequenceId)))
                : List.of();
    }

    /**
     * @deprecated use {@link #listRelationsBySourceSequenceId(String, String)} with an explicit projectId.
     */
    @Deprecated
    public List<TimeseriesRelation> listRelationsBySourceSequenceId(String sequenceId) {
        return relations.stream().filter(item -> sequenceId != null
                && item.getSourceSequences() != null && item.getSourceSequences().contains(sequenceId)).toList();
    }

    public List<TimeseriesRelation> listRelationsBySourceSequenceId(String projectId, String sequenceId) {
        return sequenceId != null && relationsBySourceSequenceId.containsKey(cacheKey(projectId, sequenceId))
                ? List.copyOf(relationsBySourceSequenceId.get(cacheKey(projectId, sequenceId)))
                : List.of();
    }

    /**
     * @deprecated use {@link #getEventByEventId(String, String)} with an explicit projectId.
     */
    @Deprecated
    public TimeseriesEvent getEventByEventId(String eventId) {
        return firstAcrossProjects("getEventByEventId",
                eventId == null
                        ? List.of()
                        : events.stream()
                                .filter(item -> eventId.equals(item.getEventId()))
                                .collect(Collectors.toList()),
                TimeseriesEvent::getProjectId)
                .orElse(null);
    }

    public TimeseriesEvent getEventByEventId(String projectId, String eventId) {
        if (eventId == null) {
            return null;
        }
        TimeseriesEvent found = eventsByEventId.get(cacheKey(projectId, eventId));
        return found != null
                && eventId.equals(found.getEventId())
                && sameProject(projectId, found.getProjectId())
                ? found : null;
    }

    /**
     * @deprecated use {@link #listAnomalyResultsByTaskId(String, String)} with an explicit projectId.
     */
    @Deprecated
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

    /**
     * @deprecated use {@link #listForecastResultsByTaskId(String, String)} with an explicit projectId.
     */
    @Deprecated
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
        relations.forEach(this::addRelationToTargetIndex);
    }

    private void rebuildRelationsBySourceId() {
        relationsBySourceSequenceId.clear();
        relations.forEach(this::addRelationToSourceIndex);
    }

    private void addRelationToIndexes(TimeseriesRelation relation) {
        if (relation == null) {
            return;
        }
        addRelationToTargetIndex(relation);
        addRelationToSourceIndex(relation);
    }

    private void addRelationToTargetIndex(TimeseriesRelation relation) {
        if (relation.getTargetSequenceId() != null) {
            relationsByTargetSequenceId
                    .computeIfAbsent(cacheKey(relation.getProjectId(), relation.getTargetSequenceId()),
                            k -> new CopyOnWriteArrayList<>())
                    .add(relation);
        }
    }

    private void addRelationToSourceIndex(TimeseriesRelation relation) {
        if (relation.getSourceSequences() != null) {
            relation.getSourceSequences().forEach(src -> {
                if (src != null) {
                    relationsBySourceSequenceId
                            .computeIfAbsent(cacheKey(relation.getProjectId(), src),
                                    k -> new CopyOnWriteArrayList<>())
                            .add(relation);
                }
            });
        }
    }

    private void removeRelationFromIndexes(TimeseriesRelation relation) {
        if (relation == null) {
            return;
        }
        if (relation.getTargetSequenceId() != null) {
            removeRelationFromIndex(relationsByTargetSequenceId,
                    cacheKey(relation.getProjectId(), relation.getTargetSequenceId()), relation);
        }
        if (relation.getSourceSequences() != null) {
            for (String src : relation.getSourceSequences()) {
                if (src != null) {
                    removeRelationFromIndex(relationsBySourceSequenceId,
                            cacheKey(relation.getProjectId(), src), relation);
                }
            }
        }
    }

    private void removeRelationFromIndex(Map<String, List<TimeseriesRelation>> index,
            String key, TimeseriesRelation relation) {
        List<TimeseriesRelation> entries = index.get(key);
        if (entries == null) {
            return;
        }
        entries.removeIf(item -> item == relation
                || (sameProject(relation.getProjectId(), item.getProjectId())
                    && relation.getRelationId() != null
                    && relation.getRelationId().equals(item.getRelationId())));
        if (entries.isEmpty()) {
            index.remove(key);
        }
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

    private ProjectCacheBucket projectBucket(String projectId) {
        return projectBuckets.computeIfAbsent(projectId, ignored -> new ProjectCacheBucket());
    }

    private void clearProjectTable(Consumer<ProjectCacheBucket> clearer) {
        projectBuckets.values().forEach(clearer);
        projectBuckets.entrySet().removeIf(entry -> isEmpty(entry.getValue()));
    }

    private boolean isEmpty(ProjectCacheBucket bucket) {
        return bucket.instanceConfigs.isEmpty()
                && bucket.categories.isEmpty()
                && bucket.constraints.isEmpty()
                && bucket.relations.isEmpty()
                && bucket.events.isEmpty()
                && bucket.anomalyTasks.isEmpty()
                && bucket.forecastTasks.isEmpty()
                && bucket.anomalyResults.isEmpty()
                && bucket.forecastResults.isEmpty()
                && bucket.syncLogs.isEmpty();
    }

    private <T> void rebuildProjectTable(
            Collection<T> records,
            Function<ProjectCacheBucket, List<T>> listGetter,
            Consumer<ProjectCacheBucket> clearer,
            Function<T, String> projectExtractor) {
        clearProjectTable(clearer);
        if (records == null) {
            return;
        }
        records.forEach(record -> {
            if (record == null) {
                return;
            }
            String projectId = projectExtractor.apply(record);
            if (projectId != null && !projectId.isBlank()) {
                listGetter.apply(projectBucket(projectId)).add(record);
            }
        });
    }

    private <T> void upsertProjectBucket(
            T entity,
            Function<ProjectCacheBucket, List<T>> listGetter,
            Predicate<T> sameBusinessKey) {
        if (entity == null) {
            return;
        }
        String projectId = projectIdOf(entity);
        if (projectId == null || projectId.isBlank()) {
            return;
        }
        upsert(listGetter.apply(projectBucket(projectId)), sameBusinessKey, entity);
    }

    private String projectIdOf(Object entity) {
        if (entity instanceof TimeseriesInstanceConfig value) return value.getProjectId();
        if (entity instanceof TimeseriesCategory value) return value.getProjectId();
        if (entity instanceof TimeseriesConstraint value) return value.getProjectId();
        if (entity instanceof TimeseriesRelation value) return value.getProjectId();
        if (entity instanceof TimeseriesEvent value) return value.getProjectId();
        if (entity instanceof TimeseriesAnomalyTask value) return value.getProjectId();
        if (entity instanceof TimeseriesForecastTask value) return value.getProjectId();
        if (entity instanceof TimeseriesAnomalyResult value) return value.getProjectId();
        if (entity instanceof TimeseriesForecastResult value) return value.getProjectId();
        if (entity instanceof TimeseriesSyncLog value) return value.getProjectId();
        return null;
    }

    private <T> List<T> projectBucketContents(
            String projectId, Function<ProjectCacheBucket, List<T>> listGetter) {
        if (projectId == null || projectId.isBlank()) {
            return List.of();
        }
        ProjectCacheBucket bucket = projectBuckets.get(projectId);
        return bucket == null ? List.of() : List.copyOf(listGetter.apply(bucket));
    }

    private <T> List<T> listForProject(
            List<T> records, String projectId, Function<T, String> projectExtractor) {
        if (projectId == null || projectId.isBlank()) {
            return List.of();
        }
        return records.stream()
                .filter(item -> projectId.equals(projectExtractor.apply(item)))
                .toList();
    }

    private boolean equalsValue(String left, String right) {
        return left != null && left.equals(right);
    }

    private boolean sameProject(String left, String right) {
        return java.util.Objects.equals(left, right);
    }

    private String cacheKey(String projectId, String id) {
        return (projectId == null ? "" : projectId) + "::" + String.valueOf(id);
    }

    /**
     * Legacy no-project fallback: return the first match, but log a warning
     * when the same id exists under multiple distinct projects (ambiguous).
     */
    private <T> Optional<T> firstAcrossProjects(String methodName, List<T> matches,
            java.util.function.Function<T, String> projectExtractor) {
        if (matches.isEmpty()) {
            return Optional.empty();
        }
        long distinctProjects = matches.stream()
                .map(projectExtractor)
                .filter(java.util.Objects::nonNull)
                .distinct()
                .count();
        if (distinctProjects > 1) {
            LOG.warn("[cache] {}: legacy no-project lookup matched {} distinct projects; "
                    + "use the project-aware overload", methodName, distinctProjects);
        }
        return Optional.of(matches.get(0));
    }
}
