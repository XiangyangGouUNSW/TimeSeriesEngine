package com.sfkg.timeseries.entity;

import java.time.LocalDateTime;
import lombok.Data;

@Data
public class TimeseriesSyncLog {

    private String id;
    private String syncType;
    private String businessId;
    private String targetService;
    private String syncStatus;
    private String errorMessage;
    private Integer retryTimes;
    private LocalDateTime lastRetryTime;
}
