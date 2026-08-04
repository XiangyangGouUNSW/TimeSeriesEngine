package com.sfkg.timeseries.entity;

import java.time.LocalDateTime;
import lombok.Data;

@Data
public class TimeseriesInstanceConfig {

    private Integer id;
    private Integer sequenceId;
    private String instanceName;
    private Integer externalSequenceId;
    private Integer categoryId;
    private String categoryName;
    private Integer deviceInstanceId;
    private String deviceInstanceName;
    private Integer dataSourceId;
    private String accessStatus;
    private LocalDateTime lastDataTime;
}
