package com.sfkg.timeseries.controller;

import com.sfkg.timeseries.common.ApiResult;
import com.sfkg.timeseries.service.TimeseriesCacheService;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/timeseries/cache")
public class TimeseriesCacheController {

    private final TimeseriesCacheService cacheService;

    public TimeseriesCacheController(TimeseriesCacheService cacheService) {
        this.cacheService = cacheService;
    }

    @PostMapping("/warm-up")
    public ApiResult<Void> warmUpAllTables() {
        cacheService.warmUpAllTables();
        return ApiResult.success(null);
    }

    @PostMapping("/tables/{tableName}/refresh")
    public ApiResult<Void> refreshTable(@PathVariable String tableName) {
        cacheService.refreshTable(tableName);
        return ApiResult.success(null);
    }
}
