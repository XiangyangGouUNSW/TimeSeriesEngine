package com.sfkg.timeseries.dto;

import java.time.LocalDateTime;
import java.util.List;
import lombok.Data;

@Data
public class StatisticsQueryRequest {
    private String projectId;

    private List<String> sequenceIds;
    private String dependentSequenceId;
    private List<String> relationIds;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
}
