package com.sfkg.timeseries.vo;

import lombok.Data;

@Data
public class InstanceConfigVO {

    private Integer sequenceId;
    private String instanceName;
    private String categoryName;
    private String deviceInstanceName;
    private String accessStatus;
}
