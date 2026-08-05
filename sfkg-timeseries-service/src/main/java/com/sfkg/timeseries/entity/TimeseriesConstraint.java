package com.sfkg.timeseries.entity;

import java.util.List;
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
    private Double lowerBound;
    private Double upperBound;
    private List<ConstraintTermItem> terms;
    private String effectiveStatus;
    private String confirmStatus;

    @Data
    public static class ConstraintTermItem {
        private String variable;
        private Double coefficient;
        private Long sampleOffset;
    }
}
