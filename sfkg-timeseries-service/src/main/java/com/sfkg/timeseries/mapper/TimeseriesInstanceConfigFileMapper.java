package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.dto.InstanceConfigQueryRequest;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import com.sfkg.timeseries.dataingest.DataIngestPersistenceService;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class TimeseriesInstanceConfigFileMapper implements TimeseriesInstanceConfigMapper {

    private final LocalJsonTableStore<TimeseriesInstanceConfig> store;
    private final DataIngestPersistenceService dataIngestPersistenceService;

    public TimeseriesInstanceConfigFileMapper(
            @Value("${timeseries.local-store-dir:data}") String storeDir,
            DataIngestPersistenceService dataIngestPersistenceService) {
        this.store = new LocalJsonTableStore<>(
                storeDir,
                "timeseries-instance-config.json",
                TimeseriesInstanceConfig.class);
        this.dataIngestPersistenceService = dataIngestPersistenceService;
    }

    @Override
    public void insert(TimeseriesInstanceConfig entity) {
        store.upsert(item -> sameBusinessKey(entity, item), entity);
        if (entity != null) {
            dataIngestPersistenceService.submitRecord("timeseries_instance_config", entity.getSequenceId(), entity);
        }
    }

    @Override
    public TimeseriesInstanceConfig selectBySequenceId(String sequenceId) {
        return store.readAll().stream()
                .filter(entity -> Objects.equals(sequenceId, entity.getSequenceId()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public List<TimeseriesInstanceConfig> selectByCondition(Object condition) {
        return store.readAll().stream()
                .filter(entity -> matches(condition, entity))
                .collect(Collectors.toList());
    }

    @Override
    public boolean existsBySequenceId(String sequenceId) {
        return selectBySequenceId(sequenceId) != null;
    }

    private boolean sameBusinessKey(TimeseriesInstanceConfig incoming, TimeseriesInstanceConfig stored) {
        if (incoming == null || stored == null) {
            return false;
        }
        return Objects.equals(incoming.getProjectId(), stored.getProjectId())
                && Objects.equals(incoming.getSequenceId(), stored.getSequenceId());
    }

    private boolean matches(Object condition, TimeseriesInstanceConfig entity) {
        if (condition == null) {
            return true;
        }
        if (!(condition instanceof InstanceConfigQueryRequest request)) {
            return true;
        }
        return equalsIfPresent(request.getSequenceId(), entity.getSequenceId())
                && equalsIfPresent(request.getCategoryId(), entity.getCategoryId())
                && equalsIfPresent(request.getDeviceInstanceId(), entity.getDeviceInstanceId())
                && equalsTextIfPresent(request.getAccessStatus(), entity.getAccessStatus());
    }

    private boolean equalsIfPresent(String expected, String actual) {
        return expected == null || Objects.equals(expected, actual);
    }

    private boolean equalsTextIfPresent(String expected, String actual) {
        return expected == null || (actual != null && expected.equalsIgnoreCase(actual));
    }
}
