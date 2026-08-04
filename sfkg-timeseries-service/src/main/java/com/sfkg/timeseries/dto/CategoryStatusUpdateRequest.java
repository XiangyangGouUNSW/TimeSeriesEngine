package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class CategoryStatusUpdateRequest {

    private Integer categoryId;
    private String confirmStatus;
    private String effectiveStatus;
}
