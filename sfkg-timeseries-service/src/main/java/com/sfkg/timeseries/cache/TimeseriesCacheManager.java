package com.sfkg.timeseries.cache;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class TimeseriesCacheManager {

    private final TimeseriesMemoryCache memoryCache;
    private final TimeseriesCacheLoader cacheLoader;

    public TimeseriesCacheManager(
            TimeseriesMemoryCache memoryCache,
            TimeseriesCacheLoader cacheLoader) {
        this.memoryCache = memoryCache;
        this.cacheLoader = cacheLoader;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void warmUpOnStartup() {
        warmUpAllTables();
    }

    public void warmUpAllTables() {
        for (CachedTable table : CachedTable.values()) {
            ensureTableLoaded(table);
        }
    }

    public synchronized void ensureTableLoaded(CachedTable table) {
        if (!memoryCache.isLoaded(table)) {
            refreshTable(table);
        }
    }

    public synchronized void refreshTable(CachedTable table) {
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

    public void invalidateTable(CachedTable table) {
        memoryCache.evict(table);
    }
}
