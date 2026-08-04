package com.sfkg.timeseries.dto;

import java.time.LocalDateTime;
import java.util.Collection;
import lombok.Data;

@Data
public class EventQueryRequest {

    private String eventType;
    private String eventSource;
    private String eventLevel;
    private String confirmStatus;
    private String handleStatus;
    private Collection<Integer> relatedSequences;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
}
