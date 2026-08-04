package com.sfkg.timeseries.entity;

import java.time.LocalDateTime;
import lombok.Data;

@Data
public class TimeseriesInstanceConfig {

    private String id;
    private String sequenceId;
    private String instanceName;
    private String externalSequenceId;
    private String categoryId;
    private String categoryName;
    private String deviceInstanceId;
    private String deviceInstanceName;
    private String dataSourceId;
    private String accessStatus;
    private LocalDateTime lastDataTime;
}
