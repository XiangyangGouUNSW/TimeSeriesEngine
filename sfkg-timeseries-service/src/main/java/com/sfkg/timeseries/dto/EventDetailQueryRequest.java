package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class EventDetailQueryRequest {
    private String projectId;

    private String eventId;
}
