package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesForecastTask;
import java.util.List;

public interface TimeseriesForecastTaskMapper {

    void insert(TimeseriesForecastTask entity);

    void updateById(TimeseriesForecastTask entity);

    TimeseriesForecastTask selectById(String taskId);

    List<TimeseriesForecastTask> selectByCondition(Object condition);

    void updateStatus(String taskId, String status);
}
