package com.sfkg.timeseries.dto;

import java.util.Collection;
import lombok.Data;

@Data
public class ForecastTaskSaveRequest {

    private Integer taskId;
    private String taskName;
    private Collection<Integer> forecastObjects;
    private String forecastHorizon;
    private String warningRule;
    private String status;
}
