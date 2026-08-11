package com.sfkg.timeseries.vo;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Collection;
import lombok.Data;

@Data
public class RelationVO {

    private String relationId;
    private String relationName;
    private Collection<String> sourceSequences;
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
