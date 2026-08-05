package com.sfkg.timeseries.entity;

import java.time.LocalDateTime;
import java.util.List;
import lombok.Data;

@Data
public class TimeseriesForecastResult {

    private String resultId;
    private String taskId;
    private String runId;
    private LocalDateTime generatedAt;
    private String status;
    private String message;
    private List<Long> timestampsMs;
    private List<String> sequenceIds;
    private List<Double> values;
    private LocalDateTime receivedTime;
}
