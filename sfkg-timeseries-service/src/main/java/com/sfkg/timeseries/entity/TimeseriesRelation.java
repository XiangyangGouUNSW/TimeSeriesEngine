package com.sfkg.timeseries.entity;

import java.math.BigDecimal;
import java.util.Collection;
import lombok.Data;

@Data
public class TimeseriesRelation {

    private String relationId;
    private String relationName;
    private Collection<String> sourceCategories;
    private String targetCategoryId;
    private String targetCategoryName;
    private String relationType;
    private String lagRange;
    private BigDecimal confidence;
    private String effectiveStatus;
    private String confirmStatus;
}
