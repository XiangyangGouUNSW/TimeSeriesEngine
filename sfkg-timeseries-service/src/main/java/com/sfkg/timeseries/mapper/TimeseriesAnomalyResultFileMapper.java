package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesAnomalyResult;
import java.util.List;
import java.util.Objects;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class TimeseriesAnomalyResultFileMapper implements TimeseriesAnomalyResultMapper {

    private final LocalJsonTableStore<TimeseriesAnomalyResult> store;

    public TimeseriesAnomalyResultFileMapper(
            @Value("${timeseries.local-store-dir:data}") String storeDir) {
        this.store = new LocalJsonTableStore<>(
                storeDir,
                "timeseries-anomaly-result.json",
                TimeseriesAnomalyResult.class);
    }

    @Override
    public void insert(TimeseriesAnomalyResult entity) {
        store.upsert(
                item -> Objects.equals(entity.getProjectId(), item.getProjectId())
                        && Objects.equals(entity.getResultId(), item.getResultId()),
                entity);
    }

    @Override
    public List<TimeseriesAnomalyResult> selectAll() {
        return store.readAll();
    }
}
