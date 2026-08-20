package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class WindowConfigSaveRequest {

    private String projectId;
    private long windowSizeMs;
}
