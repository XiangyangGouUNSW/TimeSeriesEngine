package com.sfkg.timeseries.vo;

import java.math.BigDecimal;
import java.util.Collection;
import lombok.Data;

@Data
public class RelationVO {

    private Integer relationId;
    private String relationName;
    private Collection<Integer> sourceCategories;
    private Integer targetCategoryId;
    private String targetCategoryName;
    private String relationType;
    private String lagRange;
    private BigDecimal confidence;
    private String effectiveStatus;
    private String confirmStatus;
}
