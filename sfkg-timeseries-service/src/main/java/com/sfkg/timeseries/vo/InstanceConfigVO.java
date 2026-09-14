package com.sfkg.timeseries.vo;

import java.time.LocalDateTime;
import lombok.Data;

@Data
public class InstanceConfigVO {

    private String projectId;
    private String sequenceId;
    private String instanceName;
    private String externalSequenceId;
    private String categoryId;
    private String categoryName;
    private String deviceInstanceId;
    private String deviceInstanceName;
    private String dataSourceId;
    private String accessStatus;
    private String dataType;
    private String seriesKind;
    private LocalDateTime lastDataTime;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
    private String createUser;
    private String updateUser;
}
