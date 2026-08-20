package com.sfkg.timeseries.entity;

import java.time.LocalDateTime;
import java.util.List;
import lombok.Data;

@Data
public class TimeseriesForecastTask {

    private String projectId;
    private String taskId;
    private String taskName;
    private List<String> forecastObjects;
    private List<String> featureSequenceIds;
    private String forecastHorizon;
    private Long observationWindowMs;
    private Integer minimumPoints;
    private String modelKey;
    private List<String> constraintIds;
    private String warningRule;
    private String status;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
    private String createUser;
    private String updateUser;
}
