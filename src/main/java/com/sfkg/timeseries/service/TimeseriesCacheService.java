package com.sfkg.timeseries.service;

public interface TimeseriesCacheService {

    void warmUpAllTables();

    void refreshTable(String tableName);
}
