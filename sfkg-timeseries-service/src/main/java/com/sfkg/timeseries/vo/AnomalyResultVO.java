package com.sfkg.timeseries.vo;

import lombok.Data;

@Data
public class AnomalyResultVO {

    private String resultId;
    private String taskId;
    private String sequenceId;
    private String anomalyLevel;
}
