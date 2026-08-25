package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.dto.TaskQueryRequest;
import com.sfkg.timeseries.dataingest.DataIngestPersistenceService;
import com.sfkg.timeseries.entity.TimeseriesAnomalyTask;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class TimeseriesAnomalyTaskFileMapper implements TimeseriesAnomalyTaskMapper {

    private final LocalJsonTableStore<TimeseriesAnomalyTask> store;
    private final DataIngestPersistenceService dataIngestPersistenceService;

    public TimeseriesAnomalyTaskFileMapper(
            @Value("${timeseries.local-store-dir:data}") String storeDir,
            DataIngestPersistenceService dataIngestPersistenceService) {
        this.store = new LocalJsonTableStore<>(
                storeDir,
                "timeseries-anomaly-task.json",
                TimeseriesAnomalyTask.class);
        this.dataIngestPersistenceService = dataIngestPersistenceService;
    }

    @Override
    public void insert(TimeseriesAnomalyTask entity) {
        store.upsert(item -> sameBusinessKey(entity, item), entity);
        if (entity != null) {
            dataIngestPersistenceService.submitRecord(
                    "timeseries_anomaly_task", entity.getTaskId(), entity);
        }
    }

    @Override
    public void replaceLocal(List<TimeseriesAnomalyTask> entities) {
        if (entities != null && !entities.isEmpty()) {
            store.writeAll(entities);
        }
    }

    @Override
    public void updateById(TimeseriesAnomalyTask entity) {
        insert(entity);
    }

    @Override
    public TimeseriesAnomalyTask selectById(String taskId) {
        return store.readAll().stream()
                .filter(entity -> Objects.equals(taskId, entity.getTaskId()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public List<TimeseriesAnomalyTask> selectByCondition(Object condition) {
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

    private boolean sameBusinessKey(TimeseriesAnomalyTask incoming, TimeseriesAnomalyTask stored) {
        return incoming != null
                && stored != null
                && Objects.equals(incoming.getProjectId(), stored.getProjectId())
                && Objects.equals(incoming.getTaskId(), stored.getTaskId());
    }

    private boolean matches(Object condition, TimeseriesAnomalyTask entity) {
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

    private boolean matchesKeyword(String keyword, TimeseriesAnomalyTask entity) {
        if (keyword == null) {
            return true;
        }
        return containsIfPresent(keyword, entity.getTaskName())
                || containsIfPresent(keyword, entity.getWarningRule());
    }
}
