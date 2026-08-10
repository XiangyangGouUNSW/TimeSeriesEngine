package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesConstraintResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class TimeseriesConstraintResultFileMapper implements TimeseriesConstraintResultMapper {

    private final LocalJsonTableStore<TimeseriesConstraintResult> store;

    public TimeseriesConstraintResultFileMapper(
            @Value("${timeseries.local-store-dir:data}") String storeDir) {
        this.store = new LocalJsonTableStore<>(
                storeDir,
                "timeseries-constraint-result.json",
                TimeseriesConstraintResult.class);
    }

    @Override
    public void insert(TimeseriesConstraintResult entity) {
        store.upsert(
                item -> entity.getResultId() != null
                        && entity.getResultId().equals(item.getResultId()),
                entity);
    }
}
