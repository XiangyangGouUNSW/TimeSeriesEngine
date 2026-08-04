package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesAnomalyTask;
import java.util.List;

public interface TimeseriesAnomalyTaskMapper {

    void insert(TimeseriesAnomalyTask entity);

    void updateById(TimeseriesAnomalyTask entity);

    TimeseriesAnomalyTask selectById(Integer taskId);

    List<TimeseriesAnomalyTask> selectByCondition(Object condition);

    void updateStatus(Integer taskId, String status);
}
