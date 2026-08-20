package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class TaskStatusUpdateRequest {
    private String projectId;

    private String taskId;
    private String taskType;
    private String status;
}
