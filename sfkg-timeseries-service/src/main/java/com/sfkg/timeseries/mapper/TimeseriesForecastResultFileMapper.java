package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesForecastResult;
import java.util.Objects;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class TimeseriesForecastResultFileMapper implements TimeseriesForecastResultMapper {

    private final LocalJsonTableStore<TimeseriesForecastResult> store;

    public TimeseriesForecastResultFileMapper(
            @Value("${timeseries.local-store-dir:data}") String storeDir) {
        this.store = new LocalJsonTableStore<>(
                storeDir,
                "timeseries-forecast-result.json",
                TimeseriesForecastResult.class);
    }

    @Override
    public void insert(TimeseriesForecastResult entity) {
        store.upsert(item -> sameBusinessKey(entity, item), entity);
    }

    @Override
    public java.util.List<TimeseriesForecastResult> selectAll() {
        return store.readAll();
    }

    private boolean sameBusinessKey(TimeseriesForecastResult incoming, TimeseriesForecastResult stored) {
        return incoming != null
                && stored != null
                && Objects.equals(incoming.getResultId(), stored.getResultId());
    }
}
