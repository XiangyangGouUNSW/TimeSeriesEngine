package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class RelationQueryRequest {

    private Integer relationId;
    private String relationName;
    private Integer sourceCategoryId;
    private Integer targetCategoryId;
    private String relationType;
    private String effectiveStatus;
    private String confirmStatus;
    private String keyword;
}
