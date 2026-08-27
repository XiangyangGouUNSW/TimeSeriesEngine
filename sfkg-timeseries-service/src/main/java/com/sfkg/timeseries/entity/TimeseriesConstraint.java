package com.sfkg.timeseries.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TimeseriesConstraint {

    private String projectId;
    private String constraintId;
    private String constraintName;
    private Map<String, String> variableMapping;
    private String constraintDescription;
    private String constraintExpression;
    private Double lowerBound;
    private Double upperBound;
    private List<ConstraintTermItem> terms;
    // 相同非空 id 的约束组成一个 OR 子句（组内任一满足即组满足）；空 id 独立成子句；子句间 AND
    private String orGroupId;
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
        // SAMPLE / AVERAGE / MAXIMUM / MINIMUM
        private String aggregation;
    }
}
