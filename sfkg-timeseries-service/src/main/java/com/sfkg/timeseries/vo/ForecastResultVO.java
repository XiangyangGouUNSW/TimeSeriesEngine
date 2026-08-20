package com.sfkg.timeseries.vo;

import lombok.Data;

@Data
public class ForecastResultVO {

    private String projectId;
    private String resultId;
    private String taskId;
    private String sequenceId;
    private String warningLevel;
}
