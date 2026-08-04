package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class CategoryStatusUpdateRequest {

    private String categoryId;
    private String confirmStatus;
    private String effectiveStatus;
}
