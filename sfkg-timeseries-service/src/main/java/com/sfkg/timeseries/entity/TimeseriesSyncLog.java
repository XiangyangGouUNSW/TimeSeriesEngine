package com.sfkg.timeseries.entity;

import java.time.LocalDateTime;
import lombok.Data;

@Data
public class TimeseriesSyncLog {

    private Integer id;
    private String syncType;
    private Integer businessId;
    private String targetService;
    private String syncStatus;
    private String errorMessage;
    private Integer retryTimes;
    private LocalDateTime lastRetryTime;
}
