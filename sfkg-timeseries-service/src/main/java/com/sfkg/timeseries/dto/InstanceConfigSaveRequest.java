package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class InstanceConfigSaveRequest {

    private String sequenceId;
    private String instanceName;
    private String externalSequenceId;
    private String categoryId;
    private String deviceInstanceId;
    private String dataSourceId;
    private String dataType;
    private String seriesKind;
    private String accessStatus;
    private String user;
}
