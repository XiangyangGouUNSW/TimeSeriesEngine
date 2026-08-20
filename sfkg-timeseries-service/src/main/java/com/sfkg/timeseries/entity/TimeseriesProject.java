package com.sfkg.timeseries.entity;

import java.time.LocalDateTime;
import lombok.Data;

/** Local project catalog entry used to map a project to its gStore database. */
@Data
public class TimeseriesProject {

    private String projectId;
    private String databaseName;
    private String status;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
