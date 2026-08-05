package com.sfkg.timeseries.vo;

import java.util.List;
import lombok.Data;

@Data
public class AnomalyTaskVO {

    private String taskId;
    private String taskName;
    private List<String> sequenceIds;
    private List<String> methods;
    private String warningRule;
    private Integer contextLength;
    private Long slideStepMs;
    private Integer minimumPoints;
    private String status;
}
