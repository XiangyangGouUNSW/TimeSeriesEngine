package com.sfkg.timeseries.dto;

import java.time.LocalDateTime;
import lombok.Data;

@Data
public class ForecastResultQueryRequest {

    private Integer taskId;
    private Integer sequenceId;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
}
