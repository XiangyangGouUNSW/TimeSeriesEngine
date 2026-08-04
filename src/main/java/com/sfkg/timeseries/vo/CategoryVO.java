package com.sfkg.timeseries.vo;

import lombok.Data;

@Data
public class CategoryVO {

    private Integer categoryId;
    private String categoryName;
    private String dataType;
    private String categoryDescription;
    private String applicableObjectType;
    private String defaultUnit;
    private String confirmStatus;
}
