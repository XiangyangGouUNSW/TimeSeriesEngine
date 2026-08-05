package com.sfkg.timeseries.cache;

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
import com.sfkg.timeseries.mapper.TimeseriesAnomalyResultMapper;
import com.sfkg.timeseries.mapper.TimeseriesAnomalyTaskMapper;
import com.sfkg.timeseries.mapper.TimeseriesCategoryMapper;
import com.sfkg.timeseries.mapper.TimeseriesConstraintMapper;
import com.sfkg.timeseries.mapper.TimeseriesDataFileMapper;
import com.sfkg.timeseries.mapper.TimeseriesEventMapper;
import com.sfkg.timeseries.mapper.TimeseriesForecastResultMapper;
import com.sfkg.timeseries.mapper.TimeseriesForecastTaskMapper;
import com.sfkg.timeseries.mapper.TimeseriesInstanceConfigMapper;
import com.sfkg.timeseries.mapper.TimeseriesRelationMapper;
import com.sfkg.timeseries.mapper.TimeseriesSyncLogMapper;
import java.util.List;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

@Component
public class TimeseriesCacheLoader {

    private final ObjectProvider<TimeseriesInstanceConfigMapper> instanceConfigMapper;
    private final ObjectProvider<TimeseriesCategoryMapper> categoryMapper;
    private final ObjectProvider<TimeseriesConstraintMapper> constraintMapper;
    private final ObjectProvider<TimeseriesRelationMapper> relationMapper;
    private final ObjectProvider<TimeseriesEventMapper> eventMapper;
    private final ObjectProvider<TimeseriesAnomalyTaskMapper> anomalyTaskMapper;
    private final ObjectProvider<TimeseriesAnomalyResultMapper> anomalyResultMapper;
    private final ObjectProvider<TimeseriesForecastResultMapper> forecastResultMapper;
    private final ObjectProvider<TimeseriesForecastTaskMapper> forecastTaskMapper;
    private final ObjectProvider<TimeseriesSyncLogMapper> syncLogMapper;
    private final ObjectProvider<TimeseriesDataFileMapper> dataFileMapper;

    public TimeseriesCacheLoader(
            ObjectProvider<TimeseriesInstanceConfigMapper> instanceConfigMapper,
            ObjectProvider<TimeseriesCategoryMapper> categoryMapper,
            ObjectProvider<TimeseriesConstraintMapper> constraintMapper,
            ObjectProvider<TimeseriesRelationMapper> relationMapper,
            ObjectProvider<TimeseriesEventMapper> eventMapper,
            ObjectProvider<TimeseriesAnomalyTaskMapper> anomalyTaskMapper,
            ObjectProvider<TimeseriesAnomalyResultMapper> anomalyResultMapper,
            ObjectProvider<TimeseriesForecastResultMapper> forecastResultMapper,
            ObjectProvider<TimeseriesForecastTaskMapper> forecastTaskMapper,
            ObjectProvider<TimeseriesSyncLogMapper> syncLogMapper,
            ObjectProvider<TimeseriesDataFileMapper> dataFileMapper) {
        this.instanceConfigMapper = instanceConfigMapper;
        this.categoryMapper = categoryMapper;
        this.constraintMapper = constraintMapper;
        this.relationMapper = relationMapper;
        this.eventMapper = eventMapper;
        this.anomalyTaskMapper = anomalyTaskMapper;
        this.anomalyResultMapper = anomalyResultMapper;
        this.forecastResultMapper = forecastResultMapper;
        this.forecastTaskMapper = forecastTaskMapper;
        this.syncLogMapper = syncLogMapper;
        this.dataFileMapper = dataFileMapper;
    }

    public List<TimeseriesInstanceConfig> loadInstanceConfigs() {
        TimeseriesInstanceConfigMapper mapper = instanceConfigMapper.getIfAvailable();
        return mapper == null ? List.of() : emptyIfNull(mapper.selectByCondition(null));
    }

    public List<TimeseriesCategory> loadCategories() {
        TimeseriesCategoryMapper mapper = categoryMapper.getIfAvailable();
        return mapper == null ? List.of() : emptyIfNull(mapper.selectByCondition(null));
    }

    public List<TimeseriesConstraint> loadConstraints() {
        TimeseriesConstraintMapper mapper = constraintMapper.getIfAvailable();
        return mapper == null ? List.of() : emptyIfNull(mapper.selectByCondition(null));
    }

    public List<TimeseriesRelation> loadRelations() {
        TimeseriesRelationMapper mapper = relationMapper.getIfAvailable();
        return mapper == null ? List.of() : emptyIfNull(mapper.selectByCondition(null));
    }

    public List<TimeseriesEvent> loadEvents() {
        TimeseriesEventMapper mapper = eventMapper.getIfAvailable();
        return mapper == null ? List.of() : emptyIfNull(mapper.selectByCondition(null));
    }

    public List<TimeseriesAnomalyTask> loadAnomalyTasks() {
        TimeseriesAnomalyTaskMapper mapper = anomalyTaskMapper.getIfAvailable();
        return mapper == null ? List.of() : emptyIfNull(mapper.selectByCondition(null));
    }

    public List<TimeseriesAnomalyResult> loadAnomalyResults() {
        TimeseriesAnomalyResultMapper mapper = anomalyResultMapper.getIfAvailable();
        return mapper == null ? List.of() : emptyIfNull(mapper.selectAll());
    }

    public List<TimeseriesForecastResult> loadForecastResults() {
        TimeseriesForecastResultMapper mapper = forecastResultMapper.getIfAvailable();
        return mapper == null ? List.of() : emptyIfNull(mapper.selectAll());
    }

    public List<TimeseriesForecastTask> loadForecastTasks() {
        TimeseriesForecastTaskMapper mapper = forecastTaskMapper.getIfAvailable();
        return mapper == null ? List.of() : emptyIfNull(mapper.selectByCondition(null));
    }

    public List<TimeseriesSyncLog> loadSyncLogs() {
        TimeseriesSyncLogMapper mapper = syncLogMapper.getIfAvailable();
        return mapper == null ? List.of() : emptyIfNull(mapper.selectAll());
    }

    public List<TimeseriesDataPoint> loadTimeseriesDataPoints() {
        TimeseriesDataFileMapper mapper = dataFileMapper.getIfAvailable();
        return mapper == null ? List.of() : emptyIfNull(mapper.selectByCondition(null));
    }

    private <T> List<T> emptyIfNull(List<T> records) {
        return records == null ? List.of() : records;
    }
}
