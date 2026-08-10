package com.sfkg.timeseries.controller;

import static com.sfkg.timeseries.common.JsonSuccessResponse.returnSuccess;

import com.sfkg.timeseries.client.TimeseriesCoreGrpcClient;
import com.sfkg.timeseries.common.ApiResult;
import com.sfkg.timeseries.dto.DerivedSeriesConfigSaveRequest;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/timeseries/derived-series")
public class TimeseriesDerivedSeriesController {

    private final TimeseriesCoreGrpcClient coreGrpcClient;

    public TimeseriesDerivedSeriesController(TimeseriesCoreGrpcClient coreGrpcClient) {
        this.coreGrpcClient = coreGrpcClient;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiResult<Void> syncDerivedSeries(@RequestBody DerivedSeriesConfigSaveRequest request) {
        coreGrpcClient.syncDerivedSeriesConfigs(request);
        return returnSuccess("derived series config sync success");
    }
}
