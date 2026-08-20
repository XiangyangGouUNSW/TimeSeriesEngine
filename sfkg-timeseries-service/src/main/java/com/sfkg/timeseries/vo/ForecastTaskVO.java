package com.sfkg.timeseries.vo;

import java.time.LocalDateTime;
import lombok.Data;

@Data
public class ForecastTaskVO {

    private String projectId;
    private String taskId;
    private String taskName;
    private String forecastHorizon;
    private String status;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
    private String createUser;
    private String updateUser;
}
