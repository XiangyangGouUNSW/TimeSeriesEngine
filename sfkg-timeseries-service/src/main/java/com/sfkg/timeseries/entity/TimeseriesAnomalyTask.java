package com.sfkg.timeseries.entity;

import java.time.LocalDateTime;
import java.util.List;
import lombok.Data;

@Data
public class TimeseriesAnomalyTask {

    private String taskId;
    private String taskName;
    private List<String> sequenceIds;
    private List<String> methods;
    private String warningRule;
    private Integer contextLength;
    private Long slideStepMs;
    private Integer minimumPoints;
    private List<String> constraintIds;
    private String status;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
    private String createUser;
    private String updateUser;
}
