package com.sfkg.timeseries.dto;

import java.util.Collection;
import lombok.Data;

@Data
public class RelationSaveRequest {

    private String relationId;
    private String relationName;
    private Collection<String> sourceSequences;
    private String targetSequenceId;
    private String relationType;
    private String lagRange;
    private java.math.BigDecimal confidence;
    private String relationDescription;
    private String effectiveStatus;
    private String confirmStatus;
    private String user;
}
