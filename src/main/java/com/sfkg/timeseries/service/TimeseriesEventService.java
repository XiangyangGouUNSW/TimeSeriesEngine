package com.sfkg.timeseries.service;

import com.sfkg.timeseries.dto.EventQueryRequest;
import com.sfkg.timeseries.dto.EventSaveRequest;
import com.sfkg.timeseries.entity.TimeseriesEvent;
import com.sfkg.timeseries.vo.EventDetailVO;
import com.sfkg.timeseries.vo.EventListVO;
import java.util.List;

public interface TimeseriesEventService {

    List<EventListVO> listEvents(EventQueryRequest request);

    EventDetailVO getEventDetail(Integer eventId);

    Integer saveEvent(EventSaveRequest request);

    Integer saveEventEntity(TimeseriesEvent entity);

    void validateEventRelations(EventSaveRequest request);

    EventDetailVO enrichEventDetail(Integer eventId);

    void syncEventToGraph(Integer eventId);
}
