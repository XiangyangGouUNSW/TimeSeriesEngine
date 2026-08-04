package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.dto.InstanceConfigQueryRequest;
import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class TimeseriesInstanceConfigFileMapper implements TimeseriesInstanceConfigMapper {

    private final LocalJsonTableStore<TimeseriesInstanceConfig> store;

    public TimeseriesInstanceConfigFileMapper(
            @Value("${timeseries.local-store-dir:data}") String storeDir) {
        this.store = new LocalJsonTableStore<>(
                storeDir,
                "timeseries-instance-config.json",
                TimeseriesInstanceConfig.class);
    }

    @Override
    public void insert(TimeseriesInstanceConfig entity) {
        if (entity != null && entity.getId() == null) {
            entity.setId(entity.getSequenceId());
        }
        store.upsert(item -> sameBusinessKey(entity, item), entity);
    }

    @Override
    public void updateById(TimeseriesInstanceConfig entity) {
        insert(entity);
    }

    @Override
    public TimeseriesInstanceConfig selectById(Integer id) {
        return store.readAll().stream()
                .filter(entity -> Objects.equals(id, entity.getId()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public TimeseriesInstanceConfig selectBySequenceId(Integer sequenceId) {
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
    public boolean existsBySequenceId(Integer sequenceId) {
        return selectBySequenceId(sequenceId) != null;
    }

    private boolean sameBusinessKey(TimeseriesInstanceConfig incoming, TimeseriesInstanceConfig stored) {
        if (incoming == null || stored == null) {
            return false;
        }
        if (incoming.getSequenceId() != null) {
            return Objects.equals(incoming.getSequenceId(), stored.getSequenceId());
        }
        return Objects.equals(incoming.getId(), stored.getId());
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

    private boolean equalsIfPresent(Integer expected, Integer actual) {
        return expected == null || Objects.equals(expected, actual);
    }

    private boolean equalsTextIfPresent(String expected, String actual) {
        return expected == null || (actual != null && expected.equalsIgnoreCase(actual));
    }
}
