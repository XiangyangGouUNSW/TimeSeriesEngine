package com.sfkg.timeseries.dto;

import java.util.List;
import lombok.Data;

@Data
public class AnomalyTaskSaveRequest {
    private String projectId;

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
    private String user;
}
