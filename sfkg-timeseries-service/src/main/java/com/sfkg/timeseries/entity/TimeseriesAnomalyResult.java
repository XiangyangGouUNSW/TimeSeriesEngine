package com.sfkg.timeseries.entity;

import java.time.LocalDateTime;
import java.util.List;
import lombok.Data;

@Data
public class TimeseriesAnomalyResult {

    private String projectId;
    private String resultId;
    private String taskId;
    private String eventType;
    private LocalDateTime eventTime;
    private List<String> sequenceIds;
    private List<Double> values;
    private String severity;
    private String source;
    private LocalDateTime receivedTime;
}
