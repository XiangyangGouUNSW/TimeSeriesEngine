package com.sfkg.timeseries.vo;

import java.util.Map;
import lombok.Data;

@Data
public class EventDetailVO {

    private String eventId;
    private String eventName;
    private String eventDescription;
    private Map<String, Object> semanticContext;
}
