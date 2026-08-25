package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.dto.TaskQueryRequest;
import com.sfkg.timeseries.dataingest.DataIngestPersistenceService;
import com.sfkg.timeseries.entity.TimeseriesForecastTask;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class TimeseriesForecastTaskFileMapper implements TimeseriesForecastTaskMapper {

    private final LocalJsonTableStore<TimeseriesForecastTask> store;
    private final DataIngestPersistenceService dataIngestPersistenceService;

    public TimeseriesForecastTaskFileMapper(
            @Value("${timeseries.local-store-dir:data}") String storeDir,
            DataIngestPersistenceService dataIngestPersistenceService) {
        this.store = new LocalJsonTableStore<>(
                storeDir,
                "timeseries-forecast-task.json",
                TimeseriesForecastTask.class);
        this.dataIngestPersistenceService = dataIngestPersistenceService;
    }

    @Override
    public void insert(TimeseriesForecastTask entity) {
        store.upsert(item -> sameBusinessKey(entity, item), entity);
        if (entity != null) {
            dataIngestPersistenceService.submitRecord(
                    "timeseries_forecast_task", entity.getTaskId(), entity);
        }
    }

    @Override
    public void replaceLocal(List<TimeseriesForecastTask> entities) {
        if (entities != null && !entities.isEmpty()) {
            store.writeAll(entities);
        }
    }

    @Override
    public void updateById(TimeseriesForecastTask entity) {
        insert(entity);
    }

    @Override
    public TimeseriesForecastTask selectById(String taskId) {
        return store.readAll().stream()
                .filter(entity -> Objects.equals(taskId, entity.getTaskId()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public List<TimeseriesForecastTask> selectByCondition(Object condition) {
        return store.readAll().stream()
                .filter(entity -> matches(condition, entity))
                .collect(Collectors.toList());
    }

    @Override
    public void updateStatus(String taskId, String status) {
        store.update(
                entity -> Objects.equals(taskId, entity.getTaskId()),
                entity -> entity.setStatus(status));
    }

    private boolean sameBusinessKey(TimeseriesForecastTask incoming, TimeseriesForecastTask stored) {
        return incoming != null
                && stored != null
                && Objects.equals(incoming.getProjectId(), stored.getProjectId())
                && Objects.equals(incoming.getTaskId(), stored.getTaskId());
    }

    private boolean matches(Object condition, TimeseriesForecastTask entity) {
        if (condition == null) {
            return true;
        }
        if (!(condition instanceof TaskQueryRequest request)) {
            return true;
        }
        return equalsIfPresent(request.getTaskId(), entity.getTaskId())
                && containsIfPresent(request.getTaskName(), entity.getTaskName())
                && equalsTextIfPresent(request.getStatus(), entity.getStatus())
                && matchesKeyword(request.getKeyword(), entity);
    }

    private boolean equalsIfPresent(String expected, String actual) {
        return expected == null || Objects.equals(expected, actual);
    }

    private boolean equalsTextIfPresent(String expected, String actual) {
        return expected == null || (actual != null && expected.equalsIgnoreCase(actual));
    }

    private boolean containsIfPresent(String keyword, String actual) {
        return keyword == null
                || (actual != null && actual.toLowerCase().contains(keyword.toLowerCase()));
    }

    private boolean matchesKeyword(String keyword, TimeseriesForecastTask entity) {
        if (keyword == null) {
            return true;
        }
        return containsIfPresent(keyword, entity.getTaskName())
                || containsIfPresent(keyword, entity.getForecastHorizon())
                || containsIfPresent(keyword, entity.getWarningRule());
    }
}
