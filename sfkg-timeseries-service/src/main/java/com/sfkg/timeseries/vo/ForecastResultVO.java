package com.sfkg.timeseries.vo;

import java.util.List;

import lombok.Data;

@Data
public class ForecastResultVO {

    private String projectId;
    private String resultId;
    private String taskId;
    private String sequenceId;
    private String warningLevel;

    // P 端 ForecastResult 的完整字段：status/message 用于定位「为什么没写预警」
    private String status;
    private String message;
    private List<String> sequenceIds;
    private List<Double> values;
}
