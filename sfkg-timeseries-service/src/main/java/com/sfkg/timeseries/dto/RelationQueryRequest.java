package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class RelationQueryRequest {

    private String relationId;
    private String relationName;
    private String sourceCategoryId;
    private String targetCategoryId;
    private String relationType;
    private String effectiveStatus;
    private String confirmStatus;
    private String keyword;
}
