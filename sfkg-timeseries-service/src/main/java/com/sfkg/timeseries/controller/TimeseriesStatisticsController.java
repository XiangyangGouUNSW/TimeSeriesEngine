package com.sfkg.timeseries.controller;

import static com.sfkg.timeseries.common.JsonSuccessResponse.returnSuccess;

import com.sfkg.timeseries.common.ApiResult;
import com.sfkg.timeseries.dto.StatisticsQueryRequest;
import com.sfkg.timeseries.entity.TimeseriesStatisticsResult;
import com.sfkg.timeseries.service.TimeseriesStatisticsService;
import java.util.List;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/timeseries/statistics")
public class TimeseriesStatisticsController {

    private final TimeseriesStatisticsService statisticsService;

    public TimeseriesStatisticsController(TimeseriesStatisticsService statisticsService) {
        this.statisticsService = statisticsService;
    }

    @PostMapping(value = "/compute", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<TimeseriesStatisticsResult> compute(@RequestBody StatisticsQueryRequest request) {
        return returnSuccess("statistics compute success", statisticsService.computeAndStore(request));
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<List<TimeseriesStatisticsResult>> list() {
        return returnSuccess("statistics list success", statisticsService.listResults());
    }
}
