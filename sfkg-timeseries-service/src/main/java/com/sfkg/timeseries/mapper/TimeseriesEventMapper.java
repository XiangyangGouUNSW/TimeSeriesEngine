package com.sfkg.timeseries.mapper;

import com.sfkg.timeseries.entity.TimeseriesEvent;
import java.util.List;

public interface TimeseriesEventMapper {

    void insert(TimeseriesEvent entity);

    void replaceLocal(List<TimeseriesEvent> entities);

    void updateById(TimeseriesEvent entity);

    TimeseriesEvent selectById(String eventId);

    List<TimeseriesEvent> selectByCondition(Object condition);

    void updateHandleStatus(String eventId, String status);

    void updateDiagnosisResult(String eventId, String diagnosisResult);

    void updateDisposalResult(String eventId, String disposalResult);
}
