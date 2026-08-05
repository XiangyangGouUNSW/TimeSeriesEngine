package com.sfkg.timeseries.vo;

import lombok.Data;

@Data
public class ForecastTaskVO {

    private String taskId;
    private String taskName;
    private String forecastHorizon;
    private String status;
}
