package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesForecastResult;
import java.util.List;

public interface TimeseriesForecastResultMapper {

    void insert(TimeseriesForecastResult entity);

    List<TimeseriesForecastResult> selectAll();
}
