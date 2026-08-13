package com.sfkg.timeseries.dto;

import java.time.LocalDateTime;
import java.util.List;
import lombok.Data;

@Data
public class StatisticsQueryRequest {

    private List<String> sequenceIds;
    private String dependentSequenceId;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
}
