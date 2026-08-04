package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesAnomalyTask;
import java.util.List;

public interface TimeseriesAnomalyTaskMapper {

    void insert(TimeseriesAnomalyTask entity);

    void updateById(TimeseriesAnomalyTask entity);

    TimeseriesAnomalyTask selectById(String taskId);

    List<TimeseriesAnomalyTask> selectByCondition(Object condition);

    void updateStatus(String taskId, String status);
}
