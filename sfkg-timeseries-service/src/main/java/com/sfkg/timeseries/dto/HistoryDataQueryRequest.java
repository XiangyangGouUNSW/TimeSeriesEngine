package com.sfkg.timeseries.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import java.time.LocalDateTime;
import lombok.Data;

@Data
public class HistoryDataQueryRequest {

    @JsonAlias("sequence_id")
    private Integer sequenceId;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private String granularity;
}
