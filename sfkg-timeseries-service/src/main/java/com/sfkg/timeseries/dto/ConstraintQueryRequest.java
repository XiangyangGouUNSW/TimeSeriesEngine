package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class ConstraintQueryRequest {

    private String constraintId;
    private String constraintName;
    private String categoryId;
    private String effectiveStatus;
    private String confirmStatus;
    private String keyword;
}
