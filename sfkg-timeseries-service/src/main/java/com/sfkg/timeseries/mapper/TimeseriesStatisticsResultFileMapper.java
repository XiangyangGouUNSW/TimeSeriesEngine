package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesStatisticsResult;
import java.util.List;
import java.util.Objects;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class TimeseriesStatisticsResultFileMapper implements TimeseriesStatisticsResultMapper {

    private final LocalJsonTableStore<TimeseriesStatisticsResult> store;

    public TimeseriesStatisticsResultFileMapper(
            @Value("${timeseries.local-store-dir:data}") String storeDir) {
        this.store = new LocalJsonTableStore<>(
                storeDir,
                "timeseries-statistics-result.json",
                TimeseriesStatisticsResult.class);
    }

    @Override
    public void insert(TimeseriesStatisticsResult entity) {
        store.upsert(
                item -> entity.getResultId() != null
                        && entity.getResultId().equals(item.getResultId())
                        && Objects.equals(entity.getProjectId(), item.getProjectId()),
                entity);
    }

    @Override
    public List<TimeseriesStatisticsResult> selectAll() {
        return store.readAll();
    }
}
