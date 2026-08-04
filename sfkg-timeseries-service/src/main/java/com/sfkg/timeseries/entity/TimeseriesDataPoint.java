package com.sfkg.timeseries.entity;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import lombok.Data;

@Data
public class TimeseriesDataPoint {

    private Integer sequenceId;
    private LocalDateTime timestamp;
    private BigDecimal value;
}
