package com.sfkg.timeseries.cache;

import java.util.Set;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import com.sfkg.timeseries.entity.TimeseriesProject;
import org.springframework.stereotype.Component;

/**
 * In-memory catalog of projects observed while loading or writing records.
 * The catalog is intentionally separate from business data so a future gStore
 * loader can replace the discovery source without changing cache APIs.
 */
@Component
public class TimeseriesProjectRegistry {

    private final Set<String> projectIds = ConcurrentHashMap.newKeySet();

    public Set<String> listProjectIds() {
        return Set.copyOf(projectIds);
    }

    public void register(String projectId) {
        if (projectId != null && !projectId.isBlank()) {
            projectIds.add(projectId);
        }
    }

    public void refreshFromCache(TimeseriesMemoryCache cache) {
        projectIds.clear();
        cache.listInstanceConfigs().forEach(item -> register(item.getProjectId()));
        cache.listCategories().forEach(item -> register(item.getProjectId()));
        cache.listConstraints().forEach(item -> register(item.getProjectId()));
        cache.listRelations().forEach(item -> register(item.getProjectId()));
        cache.listEvents().forEach(item -> register(item.getProjectId()));
        cache.listAnomalyTasks().forEach(item -> register(item.getProjectId()));
        cache.listForecastTasks().forEach(item -> register(item.getProjectId()));
        cache.listAnomalyResults().forEach(item -> register(item.getProjectId()));
        cache.listForecastResults().forEach(item -> register(item.getProjectId()));
        cache.listSyncLogs().forEach(item -> register(item.getProjectId()));
        cache.listTimeseriesDataPoints(null).forEach(item -> register(item.getProjectId()));
    }

    public void registerProjects(Collection<TimeseriesProject> projects) {
        if (projects != null) {
            projects.forEach(project -> register(project.getProjectId()));
        }
    }
}
