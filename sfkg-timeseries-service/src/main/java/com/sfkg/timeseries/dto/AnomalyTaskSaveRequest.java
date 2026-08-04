package com.sfkg.timeseries.dto;

import java.util.Collection;
import lombok.Data;

@Data
public class AnomalyTaskSaveRequest {

    private Integer taskId;
    private String taskName;
    private Collection<Integer> detectObjects;
    private String detectMethod;
    private String warningRule;
    private String status;
}
