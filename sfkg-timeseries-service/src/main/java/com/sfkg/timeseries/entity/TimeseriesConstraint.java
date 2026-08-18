package com.sfkg.timeseries.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TimeseriesConstraint {

    private String constraintId;
    private String constraintName;
    private Map<String, String> variableMapping;
    private String constraintDescription;
    private String constraintExpression;
    private Double lowerBound;
    private Double upperBound;
    private List<ConstraintTermItem> terms;
    private String effectiveStatus;
    private String confirmStatus;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
    private String createUser;
    private String updateUser;

    @Data
    public static class ConstraintTermItem {
        private String variable;
        private Double coefficient;
        private Long sampleOffset;
    }
}
