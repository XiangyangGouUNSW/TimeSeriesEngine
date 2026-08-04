package com.sfkg.timeseries.entity;

import java.util.Collection;
import lombok.Data;

@Data
public class TimeseriesAnomalyTask {

    private String taskId;
    private String taskName;
    private Collection<String> detectObjects;
    private String detectMethod;
    private String warningRule;
    private String status;
}
