package com.sfkg.timeseries.entity;

import java.util.Map;
import lombok.Data;

@Data
public class TimeseriesConstraint {

    private String constraintId;
    private String constraintName;
    private String categoryId;
    private Map<String, String> variableMapping;
    private String constraintDescription;
    private String constraintExpression;
    private String effectiveStatus;
    private String confirmStatus;
}
