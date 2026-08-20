package com.sfkg.timeseries.cache;

import com.sfkg.timeseries.config.DataIngestProperties;
import com.sfkg.timeseries.entity.TimeseriesCategory;
import com.sfkg.timeseries.entity.TimeseriesConstraint;
import com.sfkg.timeseries.entity.TimeseriesEvent;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.entity.TimeseriesProject;
import com.sfkg.timeseries.entity.TimeseriesRelation;
import java.util.List;
import java.util.function.BiFunction;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class TimeseriesCacheManager {

    private static final Logger LOG = LoggerFactory.getLogger(TimeseriesCacheManager.class);

    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesCacheLoader cacheLoader;
    private final TimeseriesProjectRegistry projectRegistry;
    private final DataIngestProperties dataIngestProperties;

    public TimeseriesCacheManager(
            TimeseriesMemoryCache memoryCache,
            TimeseriesCacheLoader cacheLoader,
            TimeseriesProjectRegistry projectRegistry,
            DataIngestProperties dataIngestProperties) {
        this.memoryCache = memoryCache;
        this.cacheLoader = cacheLoader;
        this.projectRegistry = projectRegistry;
        this.dataIngestProperties = dataIngestProperties;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void warmUpOnStartup() {
        warmUpAllTables();
    }

    public void warmUpAllTables() {
        List<TimeseriesProject> projects;
        if (dataIngestProperties.isReadFromGstore()) {
            try {
                projects = warmUpFromGStore();
            } catch (RuntimeException exception) {
                if (!dataIngestProperties.isFallbackToLocal()) {
                    throw exception;
                }
                LOG.warn("gStore warm-up failed, fallback to local JSON: {}", exception.getMessage());
                projects = warmUpFromLocal();
            }
        } else {
            projects = warmUpFromLocal();
        }
        projectRegistry.refreshFromCache(memoryCache);
        projectRegistry.registerProjects(projects);
    }

    private List<TimeseriesProject> warmUpFromLocal() {
        for (CachedTable table : CachedTable.values()) {
            refreshLocalTable(table);
        }
        return cacheLoader.loadActiveProjects();
    }

    private List<TimeseriesProject> warmUpFromGStore() {
        List<TimeseriesProject> projects = cacheLoader.loadActiveProjects();
        memoryCache.replaceInstanceConfigs(loadForProjects(
                projects, cacheLoader::loadInstanceConfigsFromGStore));
        memoryCache.replaceCategories(loadForProjects(
                projects, cacheLoader::loadCategoriesFromGStore));
        memoryCache.replaceConstraints(loadForProjects(
                projects, cacheLoader::loadConstraintsFromGStore));
        memoryCache.replaceRelations(loadForProjects(
                projects, cacheLoader::loadRelationsFromGStore));
        memoryCache.replaceEvents(loadForProjects(
                projects, cacheLoader::loadEventsFromGStore));

        // Tasks, results, sync logs and raw points are not currently written to gStore.
        memoryCache.replaceAnomalyTasks(cacheLoader.loadAnomalyTasks());
        memoryCache.replaceForecastTasks(cacheLoader.loadForecastTasks());
        memoryCache.replaceAnomalyResults(cacheLoader.loadAnomalyResults());
        memoryCache.replaceForecastResults(cacheLoader.loadForecastResults());
        memoryCache.replaceSyncLogs(cacheLoader.loadSyncLogs());
        memoryCache.replaceTimeseriesDataPoints(cacheLoader.loadTimeseriesDataPoints());
        return projects;
    }

    private <T> List<T> loadForProjects(
            List<TimeseriesProject> projects,
            BiFunction<String, String, List<T>> loader) {
        return projects.stream()
                .filter(project -> project.getProjectId() != null && project.getDatabaseName() != null)
                .flatMap(project -> loader.apply(project.getProjectId(), project.getDatabaseName()).stream())
                .toList();
    }

    public synchronized void ensureTableLoaded(CachedTable table) {
        if (!memoryCache.isLoaded(table)) {
            refreshTable(table);
        }
    }

    public synchronized void refreshTable(CachedTable table) {
        if (dataIngestProperties.isReadFromGstore() && isGStoreBacked(table)) {
            refreshGStoreTable(table);
            return;
        }
        refreshLocalTable(table);
    }

    private void refreshLocalTable(CachedTable table) {
        switch (table) {
            case INSTANCE_CONFIG -> memoryCache.replaceInstanceConfigs(cacheLoader.loadInstanceConfigs());
            case CATEGORY -> memoryCache.replaceCategories(cacheLoader.loadCategories());
            case CONSTRAINT -> memoryCache.replaceConstraints(cacheLoader.loadConstraints());
            case RELATION -> memoryCache.replaceRelations(cacheLoader.loadRelations());
            case EVENT -> memoryCache.replaceEvents(cacheLoader.loadEvents());
            case ANOMALY_TASK -> memoryCache.replaceAnomalyTasks(cacheLoader.loadAnomalyTasks());
            case FORECAST_TASK -> memoryCache.replaceForecastTasks(cacheLoader.loadForecastTasks());
            case ANOMALY_RESULT -> memoryCache.replaceAnomalyResults(cacheLoader.loadAnomalyResults());
            case FORECAST_RESULT -> memoryCache.replaceForecastResults(cacheLoader.loadForecastResults());
            case SYNC_LOG -> memoryCache.replaceSyncLogs(cacheLoader.loadSyncLogs());
            case TIMESERIES_DATA -> memoryCache.replaceTimeseriesDataPoints(cacheLoader.loadTimeseriesDataPoints());
        }
    }

    private void refreshGStoreTable(CachedTable table) {
        List<TimeseriesProject> projects = cacheLoader.loadActiveProjects();
        switch (table) {
            case INSTANCE_CONFIG -> memoryCache.replaceInstanceConfigs(loadForProjects(
                    projects, cacheLoader::loadInstanceConfigsFromGStore));
            case CATEGORY -> memoryCache.replaceCategories(loadForProjects(
                    projects, cacheLoader::loadCategoriesFromGStore));
            case CONSTRAINT -> memoryCache.replaceConstraints(loadForProjects(
                    projects, cacheLoader::loadConstraintsFromGStore));
            case RELATION -> memoryCache.replaceRelations(loadForProjects(
                    projects, cacheLoader::loadRelationsFromGStore));
            case EVENT -> memoryCache.replaceEvents(loadForProjects(
                    projects, cacheLoader::loadEventsFromGStore));
            default -> throw new IllegalArgumentException("table is not stored in gStore: " + table);
        }
        projectRegistry.registerProjects(projects);
    }

    private boolean isGStoreBacked(CachedTable table) {
        return table == CachedTable.INSTANCE_CONFIG
                || table == CachedTable.CATEGORY
                || table == CachedTable.CONSTRAINT
                || table == CachedTable.RELATION
                || table == CachedTable.EVENT;
    }

    public void invalidateTable(CachedTable table) {
        memoryCache.evict(table);
    }
}
