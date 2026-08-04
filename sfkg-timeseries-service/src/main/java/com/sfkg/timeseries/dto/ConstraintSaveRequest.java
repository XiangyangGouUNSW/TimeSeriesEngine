package com.sfkg.timeseries.dto;

import java.util.Map;
import lombok.Data;

@Data
public class ConstraintSaveRequest {

    private Integer constraintId;
    private String constraintName;
    private Integer categoryId;
    private Map<String, Integer> variableMapping;
    private String constraintDescription;
    private String constraintExpression;
    private String lag;
    private String effectiveStatus;
    private String confirmStatus;
}
