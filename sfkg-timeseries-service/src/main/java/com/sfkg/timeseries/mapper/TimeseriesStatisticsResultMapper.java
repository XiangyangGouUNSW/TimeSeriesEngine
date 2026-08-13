package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesStatisticsResult;
import java.util.List;

public interface TimeseriesStatisticsResultMapper {

    void insert(TimeseriesStatisticsResult entity);

    List<TimeseriesStatisticsResult> selectAll();
}
