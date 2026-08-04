package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class CategoryQueryRequest {

    private Integer categoryId;
    private String categoryName;
    private String dataType;
    private String applicableObjectType;
    private String confirmStatus;
}
