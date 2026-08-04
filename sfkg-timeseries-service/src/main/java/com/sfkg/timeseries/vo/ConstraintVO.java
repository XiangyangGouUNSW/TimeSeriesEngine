package com.sfkg.timeseries.vo;

import java.util.Map;
import lombok.Data;

@Data
public class ConstraintVO {

    private String constraintId;
    private String constraintName;
    private String categoryId;
    private Map<String, String> variableMapping;
    private String constraintDescription;
    private String constraintExpression;
    private String effectiveStatus;
    private String confirmStatus;
}
