package com.sfkg.timeseries.vo;

import java.util.Map;
import lombok.Data;

@Data
public class EventDetailVO {

    private Integer eventId;
    private String eventName;
    private String eventDescription;
    private Map<String, Object> semanticContext;
}
