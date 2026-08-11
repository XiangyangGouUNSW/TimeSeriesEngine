package com.sfkg.timeseries.dto;

import java.time.LocalDateTime;
import java.util.Collection;
import lombok.Data;

@Data
public class EventSaveRequest {

    private String eventId;
    private String eventName;
    private String eventType;
    private String eventSource;
    private String taskId;
    private Collection<String> relatedSequences;
    private Collection<String> relatedRules;
    private String eventDescription;
    private String eventLevel;
    private LocalDateTime eventTime;
    private String confirmStatus;
    private String handleStatus;
    private String diagnosisResult;
    private String diagnosisBasis;
    private String disposalResult;
    private String user;
}
