package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class RelationStatusUpdateRequest {

    private String relationId;
    private String confirmStatus;
    private String effectiveStatus;
}
