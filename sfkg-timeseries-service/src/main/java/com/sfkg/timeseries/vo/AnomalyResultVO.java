package com.sfkg.timeseries.vo;

import java.time.LocalDateTime;
import java.util.List;
import lombok.Data;

@Data
public class AnomalyResultVO {

    private String resultId;
    private String taskId;
    private String sequenceId;
    private String anomalyLevel;
    private String eventType;
    private LocalDateTime eventTime;
    private String source;
    private List<String> constraintIds;
    private List<Double> values;
}
