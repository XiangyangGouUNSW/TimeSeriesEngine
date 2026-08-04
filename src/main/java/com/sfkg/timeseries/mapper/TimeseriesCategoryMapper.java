package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesCategory;
import java.util.List;

public interface TimeseriesCategoryMapper {

    void insert(TimeseriesCategory entity);

    void updateById(TimeseriesCategory entity);

    TimeseriesCategory selectById(Integer categoryId);

    List<TimeseriesCategory> selectByCondition(Object condition);

    boolean existsConfirmedCategory(Integer categoryId);
}
