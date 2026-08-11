package com.sfkg.timeseries.entity;

import java.time.LocalDateTime;
import lombok.Data;

@Data
public class TimeseriesCategory {

    private String categoryId;
    private String categoryName;
    private String dataType;
    private String categoryDescription;
    private String applicableObjectType;
    private String defaultUnit;
    private String confirmStatus;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
    private String createUser;
    private String updateUser;
}
