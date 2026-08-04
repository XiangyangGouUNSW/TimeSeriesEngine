package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class ConstraintQueryRequest {

    private Integer constraintId;
    private String constraintName;
    private Integer categoryId;
    private String effectiveStatus;
    private String confirmStatus;
    private String keyword;
}
