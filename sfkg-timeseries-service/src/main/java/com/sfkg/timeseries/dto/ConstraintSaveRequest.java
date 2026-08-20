package com.sfkg.timeseries.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConstraintSaveRequest {
    private String projectId;

    private String constraintId;
    private String constraintName;
    private Map<String, String> variableMapping;
    private String constraintDescription;
    private String constraintExpression;
    private Double lowerBound;
    private Double upperBound;
    private List<ConstraintTermDTO> terms;
    private String effectiveStatus;
    private String confirmStatus;
    private String user;

    @Data
    public static class ConstraintTermDTO {
        private String variable;
        private Double coefficient;
        private Long sampleOffset;
    }
}
