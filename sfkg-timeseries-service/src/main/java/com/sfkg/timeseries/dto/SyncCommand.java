package com.sfkg.timeseries.dto;

import java.util.Map;
import lombok.Data;

@Data
public class SyncCommand {

    private String syncType;
    private String businessId;
    private Map<String, Object> payload;
}
