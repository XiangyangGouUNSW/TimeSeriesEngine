package com.sfkg.timeseries.entity;

import java.util.Collection;
import lombok.Data;

@Data
public class TimeseriesForecastTask {

    private String taskId;
    private String taskName;
    private Collection<String> forecastObjects;
    private Collection<String> featureSequenceIds;
    private String forecastHorizon;
    private Long observationWindowMs;
    private Integer minimumPoints;
    private String modelKey;
    private Collection<String> constraintIds;
    private String warningRule;
    private String status;
}
