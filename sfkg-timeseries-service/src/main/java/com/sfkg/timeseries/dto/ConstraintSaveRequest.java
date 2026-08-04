package com.sfkg.timeseries.dto;

import java.util.Map;
import lombok.Data;

@Data
public class ConstraintSaveRequest {

    private String constraintId;
    private String constraintName;
    private String categoryId;
    private Map<String, String> variableMapping;
    private String constraintDescription;
    private String constraintExpression;
    private String lag;
    private String effectiveStatus;
    private String confirmStatus;
}
