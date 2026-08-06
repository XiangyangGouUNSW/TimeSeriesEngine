package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class RelationQueryRequest {

    private String relationId;
    private String relationName;
    private String sourceSequenceId;
    private String targetSequenceId;
    private String relationType;
    private String effectiveStatus;
    private String confirmStatus;
    private String keyword;
}
