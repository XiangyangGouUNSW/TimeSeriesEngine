package com.sfkg.timeseries.service.impl;

import com.sfkg.timeseries.cache.CachedTable;
import com.sfkg.timeseries.cache.TimeseriesCacheManager;
import com.sfkg.timeseries.common.BusinessException;
import com.sfkg.timeseries.service.TimeseriesCacheService;
import org.springframework.stereotype.Service;

@Service
public class TimeseriesCacheServiceImpl implements TimeseriesCacheService {

    private final TimeseriesCacheManager cacheManager;

    public TimeseriesCacheServiceImpl(TimeseriesCacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @Override
    public void warmUpAllTables() {
        cacheManager.warmUpAllTables();
    }

    @Override
    public void refreshTable(String tableName) {
        CachedTable table = CachedTable.fromTableName(tableName)
                .orElseThrow(() -> new BusinessException("Unsupported cache table: " + tableName));
        cacheManager.refreshTable(table);
    }
}
