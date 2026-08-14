package com.sfkg.timeseries.entity;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
public class TimeseriesStatisticsResult {

    private String resultId;
    private List<String> sequenceIds;
    private String dependentSequenceId;
    private List<String> relationIds;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private LocalDateTime computedAt;
    // sequenceId → {metricName → value}
    private Map<String, Map<String, Double>> sequenceMetrics;
    // dependentSequenceId → {independentSequenceId → coefficient}
    private Map<String, Double> correlationCoefficients;
}
