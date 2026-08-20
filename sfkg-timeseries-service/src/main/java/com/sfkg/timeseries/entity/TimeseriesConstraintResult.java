package com.sfkg.timeseries.entity;

import java.time.LocalDateTime;
import java.util.List;
import lombok.Data;

@Data
public class TimeseriesConstraintResult {

    private String projectId;
    private String resultId;
    private LocalDateTime checkTime;
    private List<String> violatedConstraintIds;
    private List<String> sequenceIds;
    private LocalDateTime receivedTime;
}
