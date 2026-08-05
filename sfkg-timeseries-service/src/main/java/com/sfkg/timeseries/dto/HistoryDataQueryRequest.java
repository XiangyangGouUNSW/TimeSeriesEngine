package com.sfkg.timeseries.dto;

import java.time.LocalDateTime;
import java.util.List;
import lombok.Data;

@Data
public class HistoryDataQueryRequest {

    private String sequenceId;
    private List<String> sequenceIds;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private Long granularity;
}
