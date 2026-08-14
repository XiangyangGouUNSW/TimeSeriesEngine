package com.sfkg.timeseries.dto;

import java.util.List;
import lombok.Data;

@Data
public class ForecastTaskSaveRequest {

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
    private String user;
}
