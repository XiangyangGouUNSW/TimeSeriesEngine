package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesConstraint;
import java.util.List;

public interface TimeseriesConstraintMapper {

    void insert(TimeseriesConstraint entity);

    void updateById(TimeseriesConstraint entity);

    TimeseriesConstraint selectById(Integer constraintId);

    List<TimeseriesConstraint> selectByCondition(Object condition);

    void updateStatus(Integer constraintId, String status);
}
