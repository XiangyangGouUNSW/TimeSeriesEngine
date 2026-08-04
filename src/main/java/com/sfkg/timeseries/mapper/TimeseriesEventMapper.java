package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesEvent;
import java.util.List;

public interface TimeseriesEventMapper {

    void insert(TimeseriesEvent entity);

    void updateById(TimeseriesEvent entity);

    TimeseriesEvent selectById(Integer eventId);

    List<TimeseriesEvent> selectByCondition(Object condition);

    void updateHandleStatus(Integer eventId, String status);

    void updateDiagnosisResult(Integer eventId, String diagnosisResult);

    void updateDisposalResult(Integer eventId, String disposalResult);
}
