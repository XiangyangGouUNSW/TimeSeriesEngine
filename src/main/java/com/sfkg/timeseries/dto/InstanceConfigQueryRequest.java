package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class InstanceConfigQueryRequest {

    private Integer sequenceId;
    private Integer categoryId;
    private Integer deviceInstanceId;
    private String accessStatus;
}
