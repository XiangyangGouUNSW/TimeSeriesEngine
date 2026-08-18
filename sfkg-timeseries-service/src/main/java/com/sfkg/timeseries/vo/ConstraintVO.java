package com.sfkg.timeseries.vo;

import java.time.LocalDateTime;
import java.util.Map;
import lombok.Data;

@Data
public class ConstraintVO {

    private String constraintId;
    private String constraintName;
    private Map<String, String> variableMapping;
    private String constraintDescription;
    private String constraintExpression;
    private String effectiveStatus;
    private String confirmStatus;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
    private String createUser;
    private String updateUser;
}
