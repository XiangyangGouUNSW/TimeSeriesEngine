package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class InstanceConfigQueryRequest {
    private String projectId;

    private String sequenceId;
    private String categoryId;
    private String deviceInstanceId;
    private String accessStatus;
}
