package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesAnomalyResult;
import java.util.List;

public interface TimeseriesAnomalyResultMapper {

    void insert(TimeseriesAnomalyResult entity);

    List<TimeseriesAnomalyResult> selectAll();
}
