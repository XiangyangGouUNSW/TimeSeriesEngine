package com.sfkg.timeseries.vo;

import lombok.Data;

@Data
public class AnomalyTaskVO {

    private String taskId;
    private String taskName;
    private String detectMethod;
    private String status;
}
