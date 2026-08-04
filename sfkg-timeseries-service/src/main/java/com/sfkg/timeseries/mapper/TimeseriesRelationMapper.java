package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesRelation;
import java.util.List;

public interface TimeseriesRelationMapper {

    void insert(TimeseriesRelation entity);

    void updateById(TimeseriesRelation entity);

    TimeseriesRelation selectById(String relationId);

    List<TimeseriesRelation> selectByCondition(Object condition);

    void updateStatus(String relationId, String status);
}
