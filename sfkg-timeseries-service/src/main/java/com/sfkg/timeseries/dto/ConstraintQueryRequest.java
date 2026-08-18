package com.sfkg.timeseries.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConstraintQueryRequest {

    private String constraintId;
    private String constraintName;
    private String effectiveStatus;
    private String confirmStatus;
    private String keyword;
}
