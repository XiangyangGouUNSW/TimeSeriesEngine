package com.sfkg.timeseries.entity;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import lombok.Data;

@Data
public class TimeseriesRelation {

    private String relationId;
    private String relationName;
    private List<String> sourceSequences;
    private String targetSequenceId;
    private String targetCategoryName;
    private String relationType;
    private String lagRange;
    private BigDecimal confidence;
    private String relationDescription;
    private String effectiveStatus;
    private String confirmStatus;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
    private String createUser;
    private String updateUser;
}
