package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class RelationStatusUpdateRequest {

    private Integer relationId;
    private String confirmStatus;
    private String effectiveStatus;
}
