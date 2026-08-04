package com.sfkg.timeseries.cache;

import java.util.Arrays;
import java.util.Optional;

public enum CachedTable {

    INSTANCE_CONFIG("timeseries_instance_config"),
    CATEGORY("timeseries_category"),
    CONSTRAINT("timeseries_constraint"),
    RELATION("timeseries_relation"),
    EVENT("timeseries_event"),
    ANOMALY_TASK("timeseries_anomaly_task"),
    FORECAST_TASK("timeseries_forecast_task"),
    SYNC_LOG("timeseries_sync_log"),
    TIMESERIES_DATA("timeseries_data");

    private final String tableName;

    CachedTable(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public static Optional<CachedTable> fromTableName(String tableName) {
        return Arrays.stream(values())
                .filter(table -> table.tableName.equals(tableName) || table.name().equalsIgnoreCase(tableName))
                .findFirst();
    }
}
