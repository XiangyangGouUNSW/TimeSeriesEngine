package com.sfkg.timeseries.controller;

import static com.sfkg.timeseries.common.JsonSuccessResponse.returnSuccess;

import com.sfkg.timeseries.common.ApiResult;
import com.sfkg.timeseries.dto.ForecastResultQueryRequest;
import com.sfkg.timeseries.service.TimeseriesForecastResultService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/timeseries/forecast-results")
public class TimeseriesForecastResultController {

    private final TimeseriesForecastResultService forecastResultService;

    public TimeseriesForecastResultController(TimeseriesForecastResultService forecastResultService) {
        this.forecastResultService = forecastResultService;
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> queryForecastResults() {
        forecastResultService.queryForecastResults(null);
        return returnSuccess("forecast result query success");
    }

    @PostMapping(value = "/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> queryForecastResultsByJson(@RequestBody ForecastResultQueryRequest request) {
        forecastResultService.queryForecastResults(request);
        return returnSuccess("forecast result query success");
    }
}
