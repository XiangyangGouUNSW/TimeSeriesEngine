package com.sfkg.timeseries.dto;

import java.util.Collection;
import lombok.Data;

@Data
public class ForecastTaskSaveRequest {

    private String taskId;
    private String taskName;
    private Collection<String> forecastObjects;
    private String forecastHorizon;
    private String warningRule;
    private String status;
}
