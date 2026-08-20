package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesCategory;
import java.util.List;

public interface TimeseriesCategoryMapper {

    void insert(TimeseriesCategory entity);

    void replaceLocal(List<TimeseriesCategory> entities);

    void updateById(TimeseriesCategory entity);

    TimeseriesCategory selectById(String categoryId);

    List<TimeseriesCategory> selectByCondition(Object condition);

    boolean existsConfirmedCategory(String categoryId);
}
