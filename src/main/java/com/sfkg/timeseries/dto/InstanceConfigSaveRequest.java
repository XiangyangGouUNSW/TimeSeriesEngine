package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class InstanceConfigSaveRequest {

    private Integer sequenceId;
    private String instanceName;
    private Integer externalSequenceId;
    private Integer categoryId;
    private Integer deviceInstanceId;
    private Integer dataSourceId;
    private String accessStatus;
}
