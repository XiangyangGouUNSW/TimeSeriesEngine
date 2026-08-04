package com.sfkg.timeseries.dto;

import lombok.Data;

@Data
public class CategorySaveRequest {

    private String categoryId;
    private String categoryName;
    private String dataType;
    private String categoryDescription;
    private String applicableObjectType;
    private String defaultUnit;
    private String confirmStatus;
}
