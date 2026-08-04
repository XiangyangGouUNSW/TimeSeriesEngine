package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import java.util.List;

public interface TimeseriesInstanceConfigMapper {

    void insert(TimeseriesInstanceConfig entity);

    void updateById(TimeseriesInstanceConfig entity);

    TimeseriesInstanceConfig selectById(Integer id);

    TimeseriesInstanceConfig selectBySequenceId(Integer sequenceId);

    List<TimeseriesInstanceConfig> selectByCondition(Object condition);

    boolean existsBySequenceId(Integer sequenceId);
}
