package com.sfkg.timeseries.vo;

import com.sfkg.timeseries.entity.TimeseriesConstraint;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
public class ConstraintVO {

    private String projectId;
    private String constraintId;
    private String constraintName;
    private Map<String, String> variableMapping;
    private String constraintDescription;
    private String constraintExpression;
    private Double lowerBound;
    private Double upperBound;
    private List<TimeseriesConstraint.ConstraintTermItem> terms;
    private String orGroupId;
    private String effectiveStatus;
    private String confirmStatus;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
    private String createUser;
    private String updateUser;
}
