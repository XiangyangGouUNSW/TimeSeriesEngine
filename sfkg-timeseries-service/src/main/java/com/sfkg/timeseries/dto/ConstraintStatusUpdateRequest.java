package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class ConstraintStatusUpdateRequest {

    private String constraintId;
    private String confirmStatus;
    private String effectiveStatus;
}
