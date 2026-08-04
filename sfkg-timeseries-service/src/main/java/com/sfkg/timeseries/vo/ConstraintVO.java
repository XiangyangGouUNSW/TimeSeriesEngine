package com.sfkg.timeseries.vo;

import java.util.Map;
import lombok.Data;

@Data
public class ConstraintVO {

    private Integer constraintId;
    private String constraintName;
    private Integer categoryId;
    private Map<String, Integer> variableMapping;
    private String constraintDescription;
    private String constraintExpression;
    private String effectiveStatus;
    private String confirmStatus;
}
