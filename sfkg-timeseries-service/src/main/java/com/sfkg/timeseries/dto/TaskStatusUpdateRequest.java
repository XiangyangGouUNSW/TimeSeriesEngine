package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class TaskStatusUpdateRequest {

    private Integer taskId;
    private String taskType;
    private String status;
}
