package com.sfkg.timeseries.cache;

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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * One in-memory tenant/project partition. The aggregate lists in
 * {@link TimeseriesMemoryCache} remain for startup discovery and compatibility;
 * business reads use these project-local lists.
 */
final class ProjectCacheBucket {

    final List<TimeseriesInstanceConfig> instanceConfigs = new CopyOnWriteArrayList<>();
    final List<TimeseriesCategory> categories = new CopyOnWriteArrayList<>();
    final List<TimeseriesConstraint> constraints = new CopyOnWriteArrayList<>();
    final List<TimeseriesRelation> relations = new CopyOnWriteArrayList<>();
    final List<TimeseriesEvent> events = new CopyOnWriteArrayList<>();
    final List<TimeseriesAnomalyTask> anomalyTasks = new CopyOnWriteArrayList<>();
    final List<TimeseriesForecastTask> forecastTasks = new CopyOnWriteArrayList<>();
    final List<TimeseriesAnomalyResult> anomalyResults = new CopyOnWriteArrayList<>();
    final List<TimeseriesForecastResult> forecastResults = new CopyOnWriteArrayList<>();
    final List<TimeseriesSyncLog> syncLogs = new CopyOnWriteArrayList<>();
}
