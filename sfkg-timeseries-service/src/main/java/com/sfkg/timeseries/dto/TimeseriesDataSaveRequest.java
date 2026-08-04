package com.sfkg.timeseries.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import lombok.Data;

@Data
public class TimeseriesDataSaveRequest {

    @JsonAlias("sequence_id")
    private String sequenceId;

    @JsonAlias({"data", "timestampValues", "timestampValueMap"})
    private Map<String, BigDecimal> points;

    private LocalDateTime timestamp;
    private BigDecimal value;
}
