package com.sfkg.timeseries.dto;

import java.time.LocalDateTime;
import lombok.Data;

@Data
public class AnomalyResultQueryRequest {

    private Integer taskId;
    private Integer sequenceId;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private String eventLevel;
}
