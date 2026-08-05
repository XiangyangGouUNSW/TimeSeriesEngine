package com.sfkg.timeseries.dto;

import java.util.List;
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
    private Double lowerBound;
    private Double upperBound;
    private List<ConstraintTermDTO> terms;
    private String effectiveStatus;
    private String confirmStatus;

    @Data
    public static class ConstraintTermDTO {
        private String variable;
        private Double coefficient;
        private Long sampleOffset;
    }
}
