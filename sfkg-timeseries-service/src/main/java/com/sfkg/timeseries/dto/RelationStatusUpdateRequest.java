package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class RelationStatusUpdateRequest {
    private String projectId;

    private String relationId;
    private String confirmStatus;
    private String effectiveStatus;
}
