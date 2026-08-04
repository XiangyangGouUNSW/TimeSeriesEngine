package com.sfkg.timeseries.dto;

import java.util.Map;
import lombok.Data;

@Data
public class DecisionContext {

    private String eventId;
    private Map<String, Object> eventInfo;
    private Map<String, Object> semanticContext;
    private Map<String, Object> statisticsContext;
}
