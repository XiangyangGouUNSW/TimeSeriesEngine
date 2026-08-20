package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesInstanceConfig;
import java.util.List;

public interface TimeseriesInstanceConfigMapper {

    void insert(TimeseriesInstanceConfig entity);

    void replaceLocal(List<TimeseriesInstanceConfig> entities);

    TimeseriesInstanceConfig selectBySequenceId(String sequenceId);

    List<TimeseriesInstanceConfig> selectByCondition(Object condition);

    boolean existsBySequenceId(String sequenceId);
}
