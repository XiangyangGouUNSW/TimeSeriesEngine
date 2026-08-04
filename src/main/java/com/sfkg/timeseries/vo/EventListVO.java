package com.sfkg.timeseries.vo;

import lombok.Data;

@Data
public class EventListVO {

    private Integer eventId;
    private String eventName;
    private String eventType;
    private String eventLevel;
    private String handleStatus;
}
