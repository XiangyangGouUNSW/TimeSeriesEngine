package com.sfkg.timeseries.vo;

import lombok.Data;

@Data
public class AnomalyResultVO {

    private Integer resultId;
    private Integer taskId;
    private Integer sequenceId;
    private String anomalyLevel;
}
