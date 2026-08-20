package com.sfkg.timeseries.dto;

import java.time.LocalDateTime;
import lombok.Data;

@Data
public class ForecastResultQueryRequest {
    private String projectId;

    private String taskId;
    private String sequenceId;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
}
