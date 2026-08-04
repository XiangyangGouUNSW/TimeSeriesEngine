package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class TaskQueryRequest {

    private Integer taskId;
    private String taskName;
    private String taskType;
    private String status;
    private String keyword;
}
