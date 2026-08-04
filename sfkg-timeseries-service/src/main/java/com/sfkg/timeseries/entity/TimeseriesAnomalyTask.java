package com.sfkg.timeseries.entity;

import java.util.Collection;
import lombok.Data;

@Data
public class TimeseriesAnomalyTask {

    private Integer taskId;
    private String taskName;
    private Collection<Integer> detectObjects;
    private String detectMethod;
    private String warningRule;
    private String status;
}
